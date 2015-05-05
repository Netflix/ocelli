package netflix.ocelli;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.InstanceQuarantiner.IncarnationFactory;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observable.Transformer;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subscriptions.Subscriptions;

/**
 * The LoadBalancer tracks lifecycle of entities and selects the next best entity based on plugable 
 * algorithms such as round robin, choice of two, random, weighted, weighted random, etc.
 * 
 * The LoadBalancer provides two usage modes, LoadBalancer#next and LoadBalancer#toObservable.  
 * LoadBalancer#next selects the next best entity from the list of known entities based on the 
 * load balancing algorithm.  LoadBalancer#toObservable() creates an Observable that whenever
 * subscribed to will emit a single next best entity.  Use toObservable() to compose with 
 * Rx retry operators.
 * 
 * Use one of the fromXXX methods to begin setting up a load balancer builder.  The builder begins 
 * with a source of entities that can be augmented with different topologies and quarantine strategies.  
 * Finally the load balancer is created by calling build() with the desired algorithm.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class LoadBalancer<T> {

    public static class Builder<T> {
        private final Observable<Instance<T>> source;
        
        Builder(Observable<Instance<T>> source) {
            this.source = source;
        }

        /**
         * Topology to limit the number of active instances from the previously provided source.  A 
         * topology will track lifecycle of all source provided entities but only send a subset of these
         * entities to the load balancer.  
         * 
         * @param topology
         * @return
         */
        public Builder<T> withTopology(Transformer<Instance<T>, Instance<T>> topology) {
            return new Builder<T>(source.compose(topology));
        }
        
        /**
         * The quaratiner creates a new incarnation of each entity while letting the entity manage its
         * own lifecycle that onCompletes whenever the entity fails.  A separate lifecycle (Observable<Void>)
         * is tracked for each incarnation with all incarnation subject to the original entities membership
         * lifecycle.  Determination failure is deferred to the entity itself.  
         * 
         * @param factory
         * @param backoffStrategy
         * @param scheduler
         * @return
         */
        public Builder<T> withQuarantiner(IncarnationFactory<T> factory, DelayStrategy backoffStrategy, Scheduler scheduler) {
            return new Builder<T>(source.flatMap(InstanceQuarantiner.create(factory, backoffStrategy, scheduler)));
        }
        
        /**
         * The quaratiner creates a new incarnation of each entity while letting the entity manage its
         * own lifecycle that onCompletes whenever the entity fails.  A separate lifecycle (Observable<Void>)
         * is tracked for each incarnation with all incarnation subject to the original entities membership
         * lifecycle.  Determination failure is deferred to the entity itself.  
         * 
         * @param factory
         * @param backoffStrategy
         * @param scheduler
         * @return
         */
        public Builder<T> withQuarantiner(IncarnationFactory<T> factory, DelayStrategy backoffStrategy) {
            return new Builder<T>(source.flatMap(InstanceQuarantiner.create(factory, backoffStrategy, Schedulers.io())));
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
         * Construct the default LoadBalancer using the round robin load balancing strategy
         * @return
         */
        public LoadBalancer<T> buildDefault() {
            return new LoadBalancer<T>(source.compose(InstanceCollector.<T>create()), RoundRobinLoadBalancer.<T>create());
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
     * Start the builder from a stream of Instance<T> where each emitted item represents an added
     * instances and the instance's Instance#getLifecycle() onCompletes when the instance is removed.
     * 
     * The source can be managed manually via {@link InstanceManager} or may be tied directly to a hot
     * Observable from a host registry service such as Eureka.  Note that the source may itself be a 
     * composed Observable that includes transformations from one type to another.
     * 
     * @param source
     * @return
     */
    public static <T> Builder<T> fromSource(Observable<Instance<T>> source) {
        return new Builder<T>(source);
    }
    
    /**
     * Construct a load balancer builder from a stream of client snapshots.  Note that
     * T must implement hashCode() and equals() so that a proper delta may determined
     * between successive snapshots.
     * 
     * @param source
     * @return
     */
    public static <T> Builder<T> fromSnapshotSource(Observable<List<T>> source) {
        return new Builder<T>(source.compose(new SnapshotToInstance<T>()));
    }
    
    /**
     * Construct a load balancer builder from a stream of client snapshots providing
     * a custom function to extract the cache key from the type.
     * 
     * @param source
     * @return
     */
    public static <T> Builder<T> fromSnapshotSource(Observable<List<T>> source, final Func1<T, ?> keyFunc) {
        return new Builder<T>(source.compose(new SnapshotToInstance<T>(keyFunc)));
    }
    
    /**
     * Construct a load balancer builder from a fixed list of clients
     * @param clients
     * @return
     */
    public static <T> Builder<T> fromFixedSource(List<T> clients) {
        return fromSnapshotSource(Observable.just(clients));
    }
    
    private final static Subscription IDLE_SUBSCRIPTION     = Subscriptions.empty();
    private final static Subscription SHUTDOWN_SUBSCRIPTION = Subscriptions.empty();
    
    private final AtomicReference<Subscription> subscription;
    private volatile List<T>                    cache;
    private final AtomicBoolean                 isSubscribed = new AtomicBoolean(false);
    private final Observable<List<T>>           source;
    private final LoadBalancerStrategy<T>       algo;
    
    // Visible for testing only
    static <T> LoadBalancer<T> create(Observable<List<T>> source, LoadBalancerStrategy<T> algo) {
        return new LoadBalancer<T>(source, algo);
    }
    
    // Visible for testing only
    LoadBalancer(final Observable<List<T>> source, final LoadBalancerStrategy<T> algo) {
        this.source       = source;
        this.subscription = new AtomicReference<Subscription>(IDLE_SUBSCRIPTION);
        this.cache        = cache;
        this.algo         = algo;
    }
    
    public Observable<T> toObservable() {
        return Observable.create(new OnSubscribe<T>() {
            @Override
            public void call(Subscriber<? super T> s) {
                try {
                    s.onNext(next());
                    s.onCompleted();
                }
                catch (Exception e) {
                    s.onError(e);
                }
            }
        });
    }
    
    /**
     * Select the next best T from the source.  Will auto-subscribe to the source on the first
     * call to next().  shutdown() must be called to unsubscribe() from the source once the load
     * balancer is no longer used.  
     * 
     * @return
     * @throws NoSuchElementException
     */
    public T next() throws NoSuchElementException {
        // Auto-subscribe
        if (isSubscribed.compareAndSet(false, true)) {
            Subscription s = source.subscribe(new Action1<List<T>>() {
                @Override
                public void call(List<T> t1) {
                    cache = t1;
                }
            });
            
            // Prevent subscription after shutdown
            if (!subscription.compareAndSet(IDLE_SUBSCRIPTION, s)) {
                s.unsubscribe();
            }
        }

        List<T> latest = cache;
        if (latest == null) {
            throw new NoSuchElementException();
        }
        
        return algo.choose(latest);
    }
    
    /**
     * Shut down the source subscription.  This LoadBalancer may no longer be used
     * after shutdown is called.
     */
    public void shutdown() {
        Subscription s = subscription.getAndSet(SHUTDOWN_SUBSCRIPTION);
        s.unsubscribe();
        cache = null;
    }
}
