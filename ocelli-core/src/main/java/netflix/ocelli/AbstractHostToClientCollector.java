package netflix.ocelli;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import rx.Notification;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

/**
 * Liftable Operator that consumes MembershipEvents for a Host type and keeps track of 
 * an immutable list of active hosts.  For each newly identified host gets an 
 * Observable<Notification<C>> from a HostToClientLifecycleFactory which each Notification 
 * stream corresponding to a client being created (OnNext) or marked as down via a failure 
 * detector (OnError).
 * 
 * There can be multiple ClientAggregator instances, one for each load balancer partitioner,
 * which share a single HostToClientLifecycleFactory.  The ClientFactory ensures there's only 
 * one instance of a client that is reference counted by the number of subscriptions from 
 * ClientAggregator instances.
 * 
 * @author elandau
 *
 * @param <H>
 * @param <C>
 */
public abstract class AbstractHostToClientCollector<H, C> implements Operator<List<C>, MembershipEvent<H>> {

    private final HashMap<H, Subscription> subs = new HashMap<H, Subscription>();
    private final Set<C> clients = new HashSet<C>();
    private ReentrantLock lock = new ReentrantLock();
    
    protected abstract Observable<Notification<C>> createClientLifeycle(H host);
    
    @Override
    public Subscriber<? super MembershipEvent<H>> call(final Subscriber<? super List<C>> s) {
        return new Subscriber<MembershipEvent<H>>(s) {
            @Override
            public void onCompleted() {
                s.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                s.onError(e);
            }

            @Override
            public void onNext(MembershipEvent<H> event) {
                switch (event.getType()) {
                case ADD: 
                    if (subs.containsKey(event.getClient())) {
                        return;
                    }
                    
                    final AtomicReference<C> current = new AtomicReference<C>();
                    subs.put(event.getClient(), createClientLifeycle(event.getClient())
                        .doOnUnsubscribe(new Action0() {
                            @Override
                            public void call() {
                                C client = current.get();
                                if (client != null)
                                    clients.remove(client);
                            }
                        })
                        .subscribe(new Action1<Notification<C>>() {
                            @Override
                            public void call(Notification<C> n) {
                                lock.lock();
                                try {
                                    switch (n.getKind()) {
                                    case OnNext: {
                                            assert n.getValue() != null;
                                            
                                            // Set the new client and handle every possible scenario
                                            C prev = current.getAndSet(n.getValue());
                                            
                                            // Same as existing value.  We can exit without doing anything.
                                            if (prev == n.getValue()) {
                                                break;
                                            }
                                            
                                            // Client instance changed so we remove the old one.
                                            if (prev != null) {
                                                clients.remove(prev);
                                            }
                                            
                                            // Add the instance
                                            clients.add(n.getValue());
                                            
                                            s.onNext(new ArrayList<C>(clients));
                                            break;
                                        }
                                    case OnError: {
                                            C c = current.getAndSet(null);
                                            if (c != null) {
                                                if (clients.remove(c)) {
                                                    s.onNext(new ArrayList<C>(clients));
                                                }
                                            }
                                        }
                                        break;
                                        
                                    case OnCompleted:
                                        break;
                                    }
                                }
                                finally {
                                    lock.unlock();
                                }
                            }
                        }));
                    break;
                    
                case REMOVE:
                    lock.lock();
                    try {
                        Subscription sub = subs.remove(event.getClient());
                        if (sub != null) {
                            sub.unsubscribe();
                            s.onNext(new ArrayList<C>(clients));
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                    break;
                    
                default:
                    break;
                }
            }
        };
    }
}
