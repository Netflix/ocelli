package netflix.ocelli;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.functions.Connectors;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Failures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Notification;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Actions;
import rx.functions.Func1;
import rx.subscriptions.SerialSubscription;
import rx.subscriptions.Subscriptions;

/**
 * ClientFactory with built in failure detector.  ClientFactoryWithFailureDetector to use functions
 * instead of inheritance to manage a client.  This decouples the implementation form a specific
 * client interface, which may not be accessible to this API. 
 * 
 * @author elandau
 *
 * @param <C>   Client type
 */
public class FailureDetectingClientLifecycleFactory<C> implements ClientLifecycleFactory<C> {
    private static final Logger LOG = LoggerFactory.getLogger(FailureDetectingClientLifecycleFactory.class);

    public static class Builder<C> {
        private Func1<Integer, Long>        quarantineDelayStrategy = Delays.fixed(10, TimeUnit.SECONDS);
        private FailureDetectorFactory<C>   failureDetector         = Failures.never();
        private ClientConnector<C>          clientConnector         = Connectors.immediate();
        private Action1<C>                  clientShutdown          = Actions.empty();

        /**
         * Strategy used to determine the delay time in msec based on the quarantine 
         * count.  The count is incremented by one for each failure detections and reset
         * once the host is back to normal.
         */
        public Builder<C> withQuarantineStrategy(Func1<Integer, Long> quarantineDelayStrategy) {
            this.quarantineDelayStrategy = quarantineDelayStrategy;
            return this;
        }
        
        /**
         * The failure detector returns an Observable that will emit a Throwable for each 
         * failure of the client.  The load balancer will quarantine the client in response.
         * @param failureDetector
         */
        public Builder<C> withFailureDetector(FailureDetectorFactory<C> failureDetector) {
            this.failureDetector = failureDetector;
            return this;
        }
        
        /**
         * The connector can be used to prime a client prior to activating it in the connection
         * pool.  
         * @param clientConnector
         */
        public Builder<C> withClientConnector(ClientConnector<C> clientConnector) {
            this.clientConnector = clientConnector;
            return this;
        }
        
        /**
         * Strategy for shutting down a client.
         * @param clientShutdown
         * @return
         */
        public Builder<C> withClientShutdown(Action1<C> clientShutdown) {
            this.clientShutdown = clientShutdown;
            return this;
        }
        
        public FailureDetectingClientLifecycleFactory<C> build() {
            return new FailureDetectingClientLifecycleFactory<C>(
                    clientShutdown,
                    clientConnector, 
                    failureDetector, 
                    quarantineDelayStrategy);
        }
    }
        
    public static <C> Builder<C> builder() {
        return new Builder<C>();
    }
    
    private final Action1<C>                clientShutdown;
    private final FailureDetectorFactory<C> failureDetector;
    private final ClientConnector<C>        clientConnector;
    private final Func1<Integer, Long>      quarantineDelayStrategy;

    public FailureDetectingClientLifecycleFactory(
            Action1<C>                 clientShutdown,
            ClientConnector<C>         clientConnector, 
            FailureDetectorFactory<C>  failureDetector, 
            Func1<Integer, Long>       quarantineDelayStrategy) {
        this.quarantineDelayStrategy = quarantineDelayStrategy;
        this.failureDetector         = failureDetector;
        this.clientConnector         = clientConnector;
        this.clientShutdown          = clientShutdown;
    }
    
    @Override
    public Observable<Notification<C>> call(final C client) {
        return Observable.create(new OnSubscribe<C>() {
            @Override
            public void call(final Subscriber<? super C> s) {
                LOG.info("Creating client {}", client);
                final AtomicInteger quarantineCounter = new AtomicInteger();
                final SerialSubscription connectSubscription = new SerialSubscription();
                
                s.add(connectSubscription);
                
                // Failure detection
                s.add(failureDetector.call(client).subscribe(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable t1) {
                        LOG.info("Client {} failed. {}", client, t1.getMessage());
                        quarantineCounter.incrementAndGet();
                        s.onError(t1);
                        connectSubscription.set(connect(connectSubscription, quarantineCounter, client, s));
                    }
                }));
                
                // Shutdown hook
                s.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        LOG.info("Client {} shutting down", client);
                        clientShutdown.call(client);
                        s.onCompleted();
                    }
                }));
                
                connectSubscription.set(connect(connectSubscription, quarantineCounter, client, s));
            }
            
            private Subscription connect(final SerialSubscription connectSubscription, final AtomicInteger quarantineCounter, final C client, final Subscriber<? super C> s) {
                Observable<C> o = clientConnector.call(client);
                int delayCount = quarantineCounter.get();
                if (delayCount > 0) { 
                    o = o.delaySubscription(quarantineDelayStrategy.call(delayCount), TimeUnit.MILLISECONDS);
                }
                return o.subscribe(
                    new Action1<C>() {
                        @Override
                        public void call(C client) {
                            LOG.info("Client {} connected", client);
                            
                            quarantineCounter.set(0);
                            s.onNext(client);
                        }
                    },
                    new Action1<Throwable>() {
                        @Override
                        public void call(Throwable t1) {
                            LOG.info("Client {} failed. {}", client, t1.getMessage());
                            
                            quarantineCounter.incrementAndGet();
                            connectSubscription.set(connect(connectSubscription, quarantineCounter, client, s));
                        }
                    });
            }
        })
        .materialize();
    }

    public String toString() {
        return "ClientFactoryWithFailureDetector[]";
    }
}
