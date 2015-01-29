package netflix.ocelli.execute;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.functions.Retrys;
import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func0;
import rx.functions.Func1;
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
public class BackupRequestExecutionStrategy<C> extends ExecutionStrategy<C> {
    public static Func0<Integer> DEFAULT_BACKUP_TIMEOUT = new Func0<Integer>() {
        @Override
        public Integer call() {
            return 10;
        }
    };
    
    public static Func1<Boolean, Boolean> DEFAULT_GUARD = new Func1<Boolean, Boolean>() {
        @Override
        public Boolean call(Boolean isPrimary) {
            return true;
        }
    };
    
    private final LoadBalancer<C>           lb;
    private final Func0<Integer>            delay;
    private final TimeUnit                  delayUnits;
    private final Func1<Throwable, Boolean> retriableError;
    private final Scheduler                 scheduler;
    private final Func1<Boolean, Boolean>   limiter;
    
    public static class Builder<C> {
        private final LoadBalancer<C>     lb;
        private Func0<Integer>            delay          = DEFAULT_BACKUP_TIMEOUT;
        private TimeUnit                  delayUnits     = TimeUnit.MILLISECONDS;
        private Func1<Boolean, Boolean>   limiter        = DEFAULT_GUARD;
        private Func1<Throwable, Boolean> retriableError = Retrys.ALWAYS;
        private Scheduler                 scheduler      = Schedulers.computation();

        private Builder(LoadBalancer<C> lb) {
            this.lb = lb;
        }
        
        /**
         * Function to determine if an exception is retriable or not.  A non 
         * retriable exception will result in an immediate error being returned
         * while the first retriable exception on either the primary or secondary
         * request will be ignored to allow the other request to complete.
         * @param retriableError
         * @return
         */
        public Builder<C> withIsRetriableFunc(Func1<Throwable, Boolean> retriableError) {
            this.retriableError = retriableError;
            return this;
        }
        
        /**
         * Use a constant timeout for the backup requets
         * @param timeout
         * @param units
         * @return
         */
        public Builder<C> withBackupTimeout(final Integer timeout, TimeUnit units) {
            return withBackupTimeout(new Func0<Integer>() {
                @Override
                public Integer call() {
                    return timeout;
                }
            }, units);
        }
        
        /**
         * Function to determine the backup request timeout for each operation.
         * @param func
         * @param units
         * @return
         */
        public Builder<C> withBackupTimeout(Func0<Integer> func, TimeUnit units) {
            this.delay = func;
            this.delayUnits = units;
            return this;
        }
        
        /**
         * Provide an external scheduler to drive the backup timeout.  Use this
         * to test with a TestScheduler
         * 
         * @param scheduler
         * @return
         */
        public Builder<C> withScheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }
        
        /**
         * Provide a function that guards against excessive backup requests.  The function
         * receives a single argument that indicates whether the request is a the primary
         * or backup request and returns whether the operation is allowed.  
         * @param limiter
         * @return
         */
        public Builder<C> withLimiter(Func1<Boolean, Boolean> limiter) {
            this.limiter = limiter;
            return this;
        }
        
        public BackupRequestExecutionStrategy<C> build() {
            return new BackupRequestExecutionStrategy<C>(this);
        }
    }
    
    public static <C> Builder<C> builder(LoadBalancer<C> lb) {
        return new Builder<C>(lb);
    }
    
    private BackupRequestExecutionStrategy(Builder<C> builder) {
        this.lb             = builder.lb;
        this.delay          = builder.delay;
        this.retriableError = builder.retriableError;
        this.scheduler      = builder.scheduler;
        this.delayUnits     = builder.delayUnits;
        this.limiter        = builder.limiter;
    }

    @Override
    public <R> Observable<R> execute(final Func1<C, Observable<R>> operation) {
        final Observable<R> o = Observable.create(lb)
                .flatMap(operation)
                .lift(new Operator<R, R>() {
                    private AtomicBoolean first = new AtomicBoolean(true);
                    private AtomicBoolean isPrimaryCondition = new AtomicBoolean(true);
                    
                    @Override
                    public Subscriber<? super R> call(final Subscriber<? super R> s) {
                        final boolean isPrimaryRequest = isPrimaryCondition.compareAndSet(true, false);
                        
                        if (!limiter.call(isPrimaryRequest)) {
                            s.onError(new NoSuchElementException("Excessive backup requests"));
                        }
                        
                        return new SafeSubscriber<R>(s) {
                            private volatile boolean hasOnNext = false;
                            
                            @Override
                            public void onCompleted() {
                                // Propagate a NoSuchElementException on an empty stream
                                if (!hasOnNext) {
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
                            public void onNext(R t) {
                                hasOnNext = true;
                                s.onNext(t);
                            }
                        };
                    }
                });
        
        return Observable.amb(
                o, 
                o.delaySubscription(delay.call(), delayUnits, scheduler));
    }
}
