package netflix.ocelli;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;

/**
 * To be used as a flatMap on a stream of Instance<T> the quarantiner converts the Instance<T> 
 * to an Observable<Instance<T>> where each emitted item represents an active Instance<T> based
 * on a failure detector specific to T.  When failure is detected the emitted instance's lifecycle
 * will be terminated and a new Instance<T> based on the original Instance<T> (passed to the flatMap)
 * will be emitted after a configurable quarantine time.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class InstanceQuarantiner<T> implements Func1<Instance<T>, Observable<Instance<T>>> {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceQuarantiner.class);
    
    private final IncarnationFactory<T> factory;
    private final DelayStrategy         backoffStrategy;
    private final Scheduler             scheduler;
    
    /**
     * Factory for creating a new incarnation of a server based on the primary incarnation.
     * 
     * @author elandau
     *
     * @param <T>
     */
    public interface IncarnationFactory<T> {
        /**
         * 
         * @param value     The primary client instance
         * @param listener  Listener to invoke with success and failure events
         * @param lifecycle Composite of this incarnations failure detected lifecycle and membership
         * @return
         */
        public T create(T value, InstanceEventListener listener, Observable<Void> lifecycle);
    }
    
    public interface ShutdownAction<T> {
        void shutdown(T object);
    }
    
    /**
     * Create a new InstanceQuaratiner
     * 
     * @param failureActionSetter Function to call to associate the failure callback with a T
     */
    public static <T> InstanceQuarantiner<T> create(
            IncarnationFactory<T> factory,
            DelayStrategy         backoffStrategy, 
            Scheduler             scheduler) {
        return new InstanceQuarantiner<T>(factory, backoffStrategy, scheduler);
    }
    
    public InstanceQuarantiner(IncarnationFactory<T> factory, DelayStrategy backoffStrategy, Scheduler scheduler) {
        this.factory = factory;
        this.scheduler = scheduler;
        this.backoffStrategy = backoffStrategy;
    }
    
    /**
     * Metrics for a single incarnation of a client type
     */
    static class IncarnationMetrics extends InstanceEventListener {
        final Action0           shutdownAction;
        final AtomicBoolean     failed = new AtomicBoolean(false);
        final InstanceMetrics   parent;
        
        public IncarnationMetrics(InstanceMetrics parent, Action0 shutdownAction) {
            this.parent = parent;
            this.shutdownAction = shutdownAction;
        }
        
        @Override
        protected void onExecutionFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            if (failed.compareAndSet(false, true)) {
                parent.onExecutionFailed(duration, timeUnit, throwable);
                shutdownAction.call();
            }
        }

        @Override
        protected void onExecutionSuccess(long duration, TimeUnit timeUnit) {
            if (!failed.get()) {
                parent.onExecutionSuccess(duration, timeUnit);
            }
        }
    }
    
    /**
     * Metrics shared across all incarnations of a client type
     */
    static class InstanceMetrics extends InstanceEventListener {
        private AtomicInteger consecutiveFailures = new AtomicInteger();
        
        @Override
        protected void onExecutionFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            consecutiveFailures.incrementAndGet();
        }

        @Override
        protected void onExecutionSuccess(long duration, TimeUnit timeUnit) {
            consecutiveFailures.set(0);
        }
    }
    
    @Override
    public Observable<Instance<T>> call(final Instance<T> primaryInstance) {
        return Observable.create(new OnSubscribe<Instance<T>>() {
            @Override
            public void call(final Subscriber<? super Instance<T>> s) {
                final InstanceMetrics instanceMetrics = new InstanceMetrics();
                final AtomicBoolean first = new AtomicBoolean(true);
                s.add(Observable
                    .defer(new Func0<Observable<Instance<T>>>() {
                        @Override
                        public Observable<Instance<T>> call() {
                            LOG.info("Creating next incarnation of '{}'", primaryInstance.getValue());
                            final BehaviorSubject<Void> incarnationLifecycle = BehaviorSubject.create();
                            final IncarnationMetrics metrics = new IncarnationMetrics(
                                instanceMetrics, 
                                // TODO: Make this a policy
                                new Action0() {
                                    @Override
                                    public void call() {
                                        incarnationLifecycle.onCompleted();
                                    }
                                });
                            
                            // The incarnation lifecycle is tied to the main instance membership as well as failure
                            // detection
                            // TODO: Can we do this without cache?
                            final Observable<Void> lifecycle = incarnationLifecycle.ambWith(primaryInstance.getLifecycle()).cache();
                            Observable<Instance<T>> o = Observable.just(Instance.create(factory.create(primaryInstance.getValue(), metrics, lifecycle), lifecycle));
                            if (!first.compareAndSet(true, false)) {
                                long delay = backoffStrategy.get(instanceMetrics.consecutiveFailures.get());
                                o = o.delaySubscription(delay, TimeUnit.MILLISECONDS, scheduler);
                            }
                            return o;
                        }
                    })
                    .concatMap(new Func1<Instance<T>, Observable<Void>>() {
                        @Override
                        public Observable<Void> call(final Instance<T> instance) {
                            s.onNext(instance);
                            return instance.getLifecycle();
                        }
                    })
                    .repeat()
                    .takeUntil(primaryInstance.getLifecycle())
                    .subscribe());
            }
        });
    }
}