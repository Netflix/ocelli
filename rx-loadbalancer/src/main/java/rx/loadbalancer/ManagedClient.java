package rx.loadbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Notification;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.subjects.PublishSubject;
import rx.subscriptions.Subscriptions;

/**
 * Encapsulate a client and its connect state.  Each client provides a sharable event
 * stream that will stream one of the following events,
 * 
 *   onNext(Notification.onNext(client)) - for each new connected client
 *   onNext(Notification.onError)        - for each connect failure or client failure
 *   onNext(Notification.onComplete)     - for each client shutdown
 *   onComplete() when the client is finally removed
 *   
 * @author elandau
 *
 * @param <H>
 * @param <C>
 * @param <M>
 */
public class ManagedClient<H, C, M extends Action1<ClientEvent>> {

    private final H host;
    private final M metrics;
    private C client;
    
    private final PublishSubject<Notification<C>> stream = PublishSubject.create();
    private final PublishSubject<Void> shutdown = PublishSubject.create();
    private final AtomicInteger subscriberCount = new AtomicInteger();
    private final Object lock = new Object();
    private final Observable<C> connector;
    
    public ManagedClient(
            final H host, 
            final HostClientConnector<H, C> connector,
            MetricsFactory<H, M> metricsFactory) {
        this.host = host;
        this.metrics = metricsFactory.call(host, new Action0() {
            @Override
            public void call() {
                synchronized (lock) {
                    client = null;
                    stream.onNext(Notification.<C>createOnError(new RuntimeException("Host failed")));
                }
            }
        });
        
        this.connector = Observable.create(new OnSubscribe<C>() {
            private List<Subscriber<? super C>> connectSubscribers = new ArrayList<Subscriber<? super C>>();
            private Subscription connectSubscription;
            
            @Override
            public void call(final Subscriber<? super C> t1) {
                synchronized (lock) {
                    t1.add(Subscriptions.create(new Action0() {
                        @Override
                        public void call() {
                            synchronized (lock) {
                                connectSubscribers.remove(t1);
                                if (connectSubscribers.isEmpty()) {
                                    connectSubscription.unsubscribe();
                                    connectSubscription = null;
                                }
                            }
                        }
                    }));
                    
                    if (client != null) {
                        t1.onNext(client);
                        t1.onCompleted();
                    }
                    else {
                        connectSubscribers.add(t1);
                        if (connectSubscribers.size() == 1) {
                            connectSubscription = connector
                                .call(host, metrics, shutdown)
                                .unsafeSubscribe(new Subscriber<C>() {
                                    @Override
                                    public void onCompleted() {
                                    }

                                    @Override
                                    public void onError(Throwable e) {
                                    }

                                    @Override
                                    public void onNext(C t) {
                                        synchronized (lock) {
                                            client = t;
                                            stream.onNext(Notification.createOnNext(client));
                                            connectSubscribers.clear();
                                        }
                                    }
                            });
                        }
                    }
                }
            }
        });
    }
    
    /**
     * Return an observable that when subscribed to will either attempt to 
     * open a new connections, return an existing connection, or wait for 
     * a pending connect request.  The final connect notification may be 
     * received either via the returned observable or from the notifications()
     * stream
     * 
     * @return
     */
    public Observable<C> connect() {
        return this.connector;
    }
    
    /**
     * Return stream of events for this client.  
     * 
     * A client is considered 'shutdown' when all subscribers to notifications() have
     * unsubscribed.
     * @return
     */
    public Observable<Notification<C>> notifications() {
        synchronized (lock) {
            subscriberCount.incrementAndGet();
            
            Observable<Notification<C>> o;
            
            if (client != null)
                o = Observable.just(Notification.createOnNext(client)).concatWith(stream);
            else
                o = stream;
            
            return o.doOnUnsubscribe(new Action0() {
                @Override
                public void call() {
                    if (subscriberCount.decrementAndGet() == 1) {
                        stream.onCompleted();
                        shutdown.onCompleted();
                    }
                }
            });
        }
    }
    
    public H getHost() {
        return host;
    }
    
    public C getClient() {
        return client;
    }
    
    public M getMetrics() {
        return metrics;
    }
    
    public String toString() {
        return host.toString();
    }
}
