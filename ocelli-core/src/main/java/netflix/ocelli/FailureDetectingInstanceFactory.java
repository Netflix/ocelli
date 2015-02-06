package netflix.ocelli;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.functions.Connectors;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Failures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subscriptions.SerialSubscription;

public class FailureDetectingInstanceFactory<C> implements Func1<C, Observable<Boolean>> {

    private static final Logger LOG = LoggerFactory.getLogger(FailureDetectingInstanceFactory.class);

    public static class Builder<C> {
        private Func1<Integer, Long>            quarantineDelayStrategy = Delays.fixed(10, TimeUnit.SECONDS);
        private Func1<C, Observable<Throwable>> failureDetector         = Failures.never();
        private Func1<C, Observable<C>>         clientConnector         = Connectors.immediate();

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
        public Builder<C> withFailureDetector(Func1<C, Observable<Throwable>> failureDetector) {
            this.failureDetector = failureDetector;
            return this;
        }
        
        /**
         * The connector can be used to prime a client prior to activating it in the connection
         * pool.  
         * @param clientConnector
         */
        public Builder<C> withClientConnector(Func1<C, Observable<C>> clientConnector) {
            this.clientConnector = clientConnector;
            return this;
        }
        
        public FailureDetectingInstanceFactory<C> build() {
            return new FailureDetectingInstanceFactory<C>(
                    clientConnector, 
                    failureDetector, 
                    quarantineDelayStrategy);
        }
    }
        
    public static <C> Builder<C> builder() {
        return new Builder<C>();
    }
    
    private final Func1<C, Observable<Throwable>> failureDetector;
    private final Func1<C, Observable<C>>         clientConnector;
    private final Func1<Integer, Long>            quarantineDelayStrategy;

    public FailureDetectingInstanceFactory(
            Func1<C, Observable<C>>    clientConnector, 
            Func1<C, Observable<Throwable>>  failureDetector, 
            Func1<Integer, Long>       quarantineDelayStrategy) {
        this.quarantineDelayStrategy = quarantineDelayStrategy;
        this.failureDetector         = failureDetector;
        this.clientConnector         = clientConnector;
    }
    
    @Override
    public Observable<Boolean> call(final C client) {
        return Observable.create(new OnSubscribe<Boolean>() {
            @Override
            public void call(final Subscriber<? super Boolean> s) {
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
                        s.onNext(false);
                        connectSubscription.set(connect(connectSubscription, quarantineCounter, client, s));
                    }
                }));
                
                connectSubscription.set(connect(connectSubscription, quarantineCounter, client, s));
            }
            
            private Subscription connect(final SerialSubscription connectSubscription, final AtomicInteger quarantineCounter, final C client, final Subscriber<? super Boolean> s) {
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
                            s.onNext(true);
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
        });
    }

    public String toString() {
        return "FailureDetectingInstanceFactory[]";
    }
}
