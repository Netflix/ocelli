package netflix.ocelli;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.InstanceQuarantiner.IncarnationFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subscriptions.Subscriptions;

/**
 * Base for any {@link LoadBalancer} whose pool of available targets is tracked as an immutable
 * list of T that can be updated at any time.  A SettableLoadBalancer is meant to be the final
 * subscriber to an RxJava stream that manages lifecycle for targets.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class LoadBalancer<T> extends Observable<T> {

    public static class Builder<T> {
        private Observable<Instance<T>> source;
        
        Builder(Observable<Instance<T>> source) {
            this.source = source;
        }

        /**
         * Topology to limit the number of active instances
         * @param topology
         * @return
         */
        public Builder<T> withTopology(Transformer<Instance<T>, Instance<T>> topology) {
            source = source.compose(topology);
            return this;
        }
        
        /**
         * Use a quarantiner to do failure detection 
         * 
         * @param factory
         * @param backoffStrategy
         * @param scheduler
         * @return
         */
        public Builder<T> withQuarantiner(IncarnationFactory<T> factory, DelayStrategy backoffStrategy, Scheduler scheduler) {
            source = source.flatMap(InstanceQuarantiner.create(factory, backoffStrategy, scheduler));
            return this;
        }
        
        /**
         * Use a quarantiner to do failure detection 
         * 
         * @param factory
         * @param backoffStrategy
         * @param scheduler
         * @return
         */
        public Builder<T> withQuarantiner(IncarnationFactory<T> factory, DelayStrategy backoffStrategy) {
            source = source.flatMap(InstanceQuarantiner.create(factory, backoffStrategy, Schedulers.io()));
            return this;
        }
        
        /**
         * Convert the client from one type to another.  Note that topology or failure detection will 
         * still occur on the previous type
         * @param converter
         * @return
         */
        public <S> Builder<S> convertTo(Func1<Instance<T>, Instance<S>> converter) {
            return new Builder<S>(source.map(converter));
        }
        
        /**
         * Finally create a load balancer given a specified strategy such as RoundRobin or ChoiceOfTwo
         * @param strategy
         * @return
         */
        public LoadBalancer<T> build(LoadBalancerStrategy<T> strategy) {
            return new LoadBalancer<T>(source.compose(InstanceCollector.<T>create()), strategy);
        }
    }
    
    /**
     * Start the builder from a source of instances
     * @param source
     * @return
     */
    public static <T> Builder<T> fromSource(Observable<Instance<T>> source) {
        return new Builder<T>(source);
    }

    private final static Subscription              IDLE_SUBSCRIPTION = Subscriptions.empty();
    private final static Subscription              SHUTDOWN_SUBSCRIPTION = Subscriptions.empty();
    
    private final    AtomicReference<Subscription> subscription;
    private final    AtomicReference<List<T>>      cache;
    
    public static <T> LoadBalancer<T> create(Observable<List<T>> source, LoadBalancerStrategy<T> algo) {
        return new LoadBalancer<T>(source, algo);
    }
    
    public LoadBalancer(final Observable<List<T>> source, final LoadBalancerStrategy<T> algo) {
        this(source, algo, new AtomicReference<Subscription>(IDLE_SUBSCRIPTION), new AtomicReference<List<T>>());
    }
    
    private LoadBalancer(
            final Observable<List<T>> source, 
            final LoadBalancerStrategy<T> algo, 
            final AtomicReference<Subscription> subscription, 
            final AtomicReference<List<T>> cache) {
        super(new OnSubscribe<T>() {
            private final AtomicBoolean isSubscribed = new AtomicBoolean(false);
            @Override
            public void call(Subscriber<? super T> o) {
                // Auto-subscribe
                if (isSubscribed.compareAndSet(false, true)) {
                    Subscription s = source.subscribe(new Action1<List<T>>() {
                        @Override
                        public void call(List<T> t1) {
                            cache.set(t1);
                        }
                    });
                    
                    // Prevent subscription after shutdown
                    if (!subscription.compareAndSet(IDLE_SUBSCRIPTION, s)) {
                        s.unsubscribe();
                    }
                }
                
                List<T> latest = cache.get();
                if (latest == null) {
                    o.onError(new NoSuchElementException());
                    return;
                }
                
                try {
                    o.onNext(algo.choose(latest));
                    o.onCompleted();
                }
                catch (Exception e) {
                    o.onError(e);
                }
            }
        });
        
        this.subscription = subscription;
        this.cache        = cache;
    }
    
    /**
     * Shut down the source subscription.  This LoadBalancer may no longer be used
     * after shutdown is called.
     */
    public void shutdown() {
        Subscription s = subscription.getAndSet(SHUTDOWN_SUBSCRIPTION);
        s.unsubscribe();
        cache.set(null);
    }
}
