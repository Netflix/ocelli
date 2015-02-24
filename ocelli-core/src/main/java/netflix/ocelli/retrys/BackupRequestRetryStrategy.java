package netflix.ocelli.retrys;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.functions.Metrics;
import netflix.ocelli.functions.Retrys;
import netflix.ocelli.functions.Stopwatches;
import netflix.ocelli.util.SingleMetric;
import netflix.ocelli.util.Stopwatch;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observable.Transformer;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

/**
 * Retry strategy that kicks off a second request if the first request does not
 * respond within an expected amount of time.  The original request remains in 
 * flight until either one responds.  The strategy tracks response latencies and 
 * feeds them into a SingleMetric that is used to determine the backup request
 * timeout.  A common metric to use is the 90th percentile response time. 
 * 
 * Note that the same BackupRequestRetryStrategy instance is stateful and should 
 * be used for all requests.  Multiple BackupRequestRetryStrategy instances may be
 * used for different request types known to have varying response latency 
 * distributions.
 * 
 * Usage,
 * 
 * {@code
 * <pre>
 * 
 * BackupRequestRetryStrategy strategy = BackupRequestRetryStrategy.builder()
 *                  .withTimeoutMetric(Metrics.quantile(0.90))
 *                  .withIsRetriablePolicy(somePolicyThatReturnsTrueOnRetriableErrors)
 *                  .build();
 *                  
 * loadBalancer
 *      .flatMap(operation)
 *      .compose(strategy)
 *      .subscribe(responseHandler)
 * </pre>
 * code}
 * 
 * @author elandau
 *
 * @param <T>
 */
public class BackupRequestRetryStrategy<T> implements Transformer<T, T> {
    public static Func0<Stopwatch>        DEFAULT_CLOCK   = Stopwatches.systemNano();
    
    private final Func0<Stopwatch>          sw;
    private final SingleMetric<Long>        metric;
    private final Func1<Throwable, Boolean> retriableError;
    private final Scheduler                 scheduler;
    
    public static class Builder<T> {
        private Func0<Stopwatch>          sw             = DEFAULT_CLOCK;
        private SingleMetric<Long>        metric         = Metrics.memoize(10L);
        private Func1<Throwable, Boolean> retriableError = Retrys.ALWAYS;
        private Scheduler                 scheduler      = Schedulers.computation();

        /**
         * Function to determine if an exception is retriable or not.  A non 
         * retriable exception will result in an immediate error being returned
         * while the first retriable exception on either the primary or secondary
         * request will be ignored to allow the other request to complete.
         * @param retriableError
         */
        public Builder<T> withIsRetriablePolicy(Func1<Throwable, Boolean> retriableError) {
            this.retriableError = retriableError;
            return this;
        }
        
        /**
         * Function to determine the backup request timeout for each operation.
         * @param func
         * @param units
         */
        public Builder<T> withTimeoutMetric(SingleMetric<Long> metric) {
            this.metric = metric;
            return this;
        }
        
        /**
         * Provide an external scheduler to drive the backup timeout.  Use this
         * to test with a TestScheduler
         * 
         * @param scheduler
         */
        public Builder<T> withScheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }
        
        /**
         * Factory for creating stopwatches.  A new stopwatch is created per operation.
         * @param clock
         */
        public Builder<T> withStopwatch(Func0<Stopwatch> sw) {
            this.sw = sw;
            return this;
        }
        
        public BackupRequestRetryStrategy<T> build() {
            return new BackupRequestRetryStrategy<T>(this);
        }
    }
    
    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }
    
    private BackupRequestRetryStrategy(Builder<T> builder) {
        this.metric         = builder.metric;
        this.retriableError = builder.retriableError;
        this.scheduler      = builder.scheduler;
        this.sw             = builder.sw;
    }
    
    @Override
    public Observable<T> call(final Observable<T> o) {
        Observable<T> timedO = Observable.create(new OnSubscribe<T>() {
            @Override
            public void call(Subscriber<? super T> s) {
                final Stopwatch timer = sw.call();
                o.doOnNext(new Action1<T>() {
                    @Override
                    public void call(T t1) {
                        metric.add(timer.elapsed(TimeUnit.MILLISECONDS));
                    }
                }).subscribe(s);
            }
        });
        
        return Observable
            .just(timedO, timedO.delaySubscription(metric.get(), TimeUnit.MILLISECONDS, scheduler))
            .flatMap(new Func1<Observable<T>, Observable<T>>() {
                final AtomicInteger counter = new AtomicInteger();
                
                @Override
                public Observable<T> call(Observable<T> t1) {
                    return t1.onErrorResumeNext(new Func1<Throwable, Observable<T>>() {
                        @Override
                        public Observable<T> call(Throwable e) {
                            if (counter.incrementAndGet() == 2 || !retriableError.call(e)) {
                                return Observable.error(e);
                            }
                            return Observable.never();
                        }
                    });
                }
            })
            .take(1);
    }
}
