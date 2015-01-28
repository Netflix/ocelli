package netflix.ocelli;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Notification;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.subjects.BehaviorSubject;
import rx.subscriptions.Subscriptions;

/**
 * Factory for creating clients from a host address description with an internal pool
 * 
 * @author elandau
 *
 * @param <H>
 * @param <C>
 */
public class HostToClientCachingLifecycleFactory<H, C> implements HostToClientLifecycleFactory<H, C> {
    private static final Logger LOG = LoggerFactory.getLogger(HostToClientCachingLifecycleFactory.class);
    
    private final HashMap<H, Observable<Notification<C>>> clients = new HashMap<H, Observable<Notification<C>>>();
    private final HostToClient<H, C> creator;
    private final ClientLifecycleFactory<C> lifecycle;
    
    public static <C> HostToClientCachingLifecycleFactory<C, C> create(ClientLifecycleFactory<C> lifecycle) {
        return new HostToClientCachingLifecycleFactory<C, C>(new HostToClient<C, C>() {
            @Override
            public C call(C t1) {
                return t1;
            }
        }, lifecycle);
    }
    
    public HostToClientCachingLifecycleFactory(HostToClient<H, C> creator, ClientLifecycleFactory<C> lifecycle) {
        this.creator = creator;
        this.lifecycle = lifecycle;
    }
    
    /**
     * Acquire a client for the specified host address.  Create a new client if one does
     * not already exist or returns a cached client.
     * 
     * @param host
     * @return
     */
    @Override
    public synchronized Observable<Notification<C>> call(final H host) {
        Observable<Notification<C>> o = clients.get(host);
        if (null == o) {
            final C client = creator.call(host);
            final AtomicInteger refCount = new AtomicInteger(0);
            final BehaviorSubject<Notification<C>> bs = BehaviorSubject.create();
            final Subscription sub = lifecycle.call(client).subscribe(bs);
            
            LOG.info("Creating lifecycle for {}", host);
            
            o = Observable.create(new OnSubscribe<Notification<C>>() {
                @Override
                public void call(Subscriber<? super Notification<C>> s) {
                    LOG.info("Connecting lifecycle for {} ({})", host, refCount.get());
                    
                    final Subscription local = bs.subscribe(s);
                    refCount.incrementAndGet();
                    
                    s.add(Subscriptions.create(new Action0() {
                        @Override
                        public void call() {
                            LOG.info("Disconnecting lifecycle for {} ({})", host, refCount.get());
                            if (refCount.decrementAndGet() == 0) {
                                LOG.info("Terminating lifecycle for {}", host);
                                bs.onCompleted();
                                sub.unsubscribe();
                                clients.remove(host);
                            }
                            local.unsubscribe();
                        }
                    }));
                }
            });
            
            clients.put(host, o);
        }

        return o;
    }
}
