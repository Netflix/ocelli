package netflix.ocelli.executor;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.functions.Metrics;
import netflix.ocelli.functions.Retrys;
import netflix.ocelli.functions.Stopwatches;
import netflix.ocelli.util.SingleMetric;
import netflix.ocelli.util.Stopwatch;
import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observers.SafeSubscriber;
import rx.schedulers.Schedulers;

/**
 * Execution strategy that executes on one host but then tries one more host
 * if there's no response after a certain timeout.  The original request is
 * kept open to allow it to complete in case it will still respond faster 
 * than the backup request.
 * 
 * @author elandau
 *
 * @param <C>
 */
public class BackupExecutor<C, I, O> implements Executor<I, O> {
    public static Func0<Stopwatch>        DEFAULT_CLOCK   = Stopwatches.systemNano();
    public static Func1<Boolean, Boolean> DEFAULT_LIMITER = new Func1<Boolean, Boolean>() {
        @Override
        public Boolean call(Boolean isPrimary) {
            return true;
        }
    };
    
    private final Func0<Stopwatch>          sw;
    private final Observable<C>             lb;
    private final SingleMetric<Long>        metric;
    private final Func1<Throwable, Boolean> retriableError;
    private final Scheduler                 scheduler;
    private final Func1<Boolean, Boolean>   limiter;
    private final Func2<C, I, Observable<O>> operation;
    
    public static class Builder<C, I, O> {
        private final LoadBalancer<C>     lb;
        private Func0<Stopwatch>          sw             = DEFAULT_CLOCK;
        private SingleMetric<Long>        metric         = Metrics.memoize(10L);
        private Func1<Boolean, Boolean>   limiter        = DEFAULT_LIMITER;
        private Func1<Throwable, Boolean> retriableError = Retrys.ALWAYS;
        private Scheduler                 scheduler      = Schedulers.computation();
        private Func2<C, I, Observable<O>> operation;

        private Builder(LoadBalancer<C> lb) {
            this.lb = lb;
        }
        
        /**
         * Function that executes the request I on specific client instance C
         * @param operation
         * @return
         */
        public Builder<C, I, O> withClientFunc(Func2<C, I, Observable<O>> operation) {
            this.operation = operation;
            return this;
        }
        
        /**
         * Function to determine if an exception is retriable or not.  A non 
         * retriable exception will result in an immediate error being returned
         * while the first retriable exception on either the primary or secondary
         * request will be ignored to allow the other request to complete.
         * @param retriableError
         */
        public Builder<C, I, O> withIsRetriableFunc(Func1<Throwable, Boolean> retriableError) {
            this.retriableError = retriableError;
            return this;
        }
        
        /**
         * Function to determine the backup request timeout for each operation.
         * @param func
         * @param units
         */
        public Builder<C, I, O> withTimeoutMetric(SingleMetric<Long> metric) {
            this.metric = metric;
            return this;
        }
        
        /**
         * Provide an external scheduler to drive the backup timeout.  Use this
         * to test with a TestScheduler
         * 
         * @param scheduler
         */
        public Builder<C, I, O> withScheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }
        
        /**
         * Provide a function that guards against excessive backup requests.  The function
         * receives a single argument that indicates whether the request is a the primary
         * or backup request and returns whether the operation is allowed.  
         * @param limiter
         */
        public Builder<C, I, O> withLimiter(Func1<Boolean, Boolean> limiter) {
            this.limiter = limiter;
            return this;
        }
        
        /**
         * Factory for creating stopwatches.  A new stopwatch is created per operation.
         * @param clock
         */
        public Builder<C, I, O> withStopwatch(Func0<Stopwatch> sw) {
            this.sw = sw;
            return this;
        }
        
        public BackupExecutor<C, I, O> build() {
            assert operation != null;
            return new BackupExecutor<C, I, O>(this);
        }
    }
    
    public static <C, I, O> Builder<C, I, O> builder(LoadBalancer<C> lb) {
        return new Builder<C, I, O>(lb);
    }
    
    private BackupExecutor(Builder<C, I, O> builder) {
        this.lb             = builder.lb;
        this.metric         = builder.metric;
        this.retriableError = builder.retriableError;
        this.scheduler      = builder.scheduler;
        this.limiter        = builder.limiter;
        this.sw             = builder.sw;
        this.operation      = builder.operation;
    }
    
    @Override
    public Observable<O> call(final I request) {
        final Observable<O> o = lb
                .concatMap(new Func1<C, Observable<O>>() {
                    @Override
                    public Observable<O> call(C t1) {
                        return operation.call(t1, request);
                    }
                })
                .lift(new Operator<O, O>() {
                    private AtomicBoolean first = new AtomicBoolean(true);
                    private AtomicBoolean isPrimaryCondition = new AtomicBoolean(true);
                    
                    @Override
                    public Subscriber<? super O> call(final Subscriber<? super O> s) {
                        final boolean isPrimaryRequest = isPrimaryCondition.compareAndSet(true, false);
                        
                        if (!limiter.call(isPrimaryRequest)) {
                            s.onError(new NoSuchElementException("Excessive backup requests"));
                        }
                        
                        final Stopwatch timer = sw.call();
                        
                        return new SafeSubscriber<O>(s) {
                            private AtomicBoolean hasOnNext = new AtomicBoolean(false);
                            
                            @Override
                            public void onCompleted() {
                                // Propagate a NoSuchElementException on an empty stream
                                if (!hasOnNext.get()) {
                                    onError(new NoSuchElementException("Stream completed with no data"));
                                }
                                else {
                                    s.onCompleted();
                                }
                            }

                            @Override
                            public void onError(Throwable e) {
                                // Ignore the first error we see as long as it's a retriable error.  This
                                // will catch situations where the primary request results in a retriable exception
                                // such as a throttle error or socket disconnect and allow the backup request
                                // to proceed.
                                // TODO: Optimize this so that the backup request is called immediately on
                                // an empty response from the first request
                                if (!first.compareAndSet(true, false) || (!(e instanceof NoSuchElementException) && !retriableError.call(e))) {
                                    s.onError(e);
                                }
                            }

                            @Override
                            public void onNext(O t) {
                                if (hasOnNext.compareAndSet(false, true)) {
                                    metric.add(timer.elapsed(TimeUnit.MILLISECONDS));
                                }
                                s.onNext(t);
                            }
                        };
                    }
                });
        
        return Observable.amb(
                o, 
                o.delaySubscription(metric.get(), TimeUnit.MILLISECONDS, scheduler));
    }
}
