package netflix.ocelli.execute;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.Subscriptions;

/**
 * Execution strategy that executes on one host but then tries one more host
 * if there's no response after a certain timeout.  The previous request is
 * kept open until to allow the original request to complete in case it will
 * still response faster than the backup request.
 * 
 * @author elandau
 *
 * TODO: Retry each operation if it fails so that the first failure doesn't kill the entire request
 * 
 * @param <C>
 */
public class BackupRequestExecutionStrategy<C> extends ExecutionStrategy<C> {
    public static Func1<Throwable, Boolean> ALWAYS = new Func1<Throwable, Boolean>() {
        @Override
        public Boolean call(Throwable e) {
            return true;
        }
    };
    
    private LoadBalancer<C> lb;
    private Func0<Integer> delay;
    private Func1<Throwable, Boolean> retriableError;
    private Scheduler scheduler = Schedulers.computation();
    private TimeUnit units = TimeUnit.MILLISECONDS;
    
    public BackupRequestExecutionStrategy(final LoadBalancer<C> lb, final Func0<Integer> delay, Func1<Throwable, Boolean> retriableError) {
        this.lb = lb;
        this.delay = delay;
        this.retriableError = retriableError;
    }
    
    public BackupRequestExecutionStrategy(final LoadBalancer<C> lb, final Func0<Integer> delay) {
        this(lb, delay, ALWAYS);
    }
    
    @Override
    public <R> Observable<R> execute(final Func1<C, Observable<R>> operation) {
        return Observable.create(new OnSubscribe<R>() {
            @Override
            public void call(final Subscriber<? super R> s) {
                final CompositeSubscription cs = new CompositeSubscription();
                final AtomicInteger requestCount = new AtomicInteger(0);
                
                // Observer for both the initial and backup request
                final Observer<R> opObserver = new Observer<R>() {
                    private AtomicInteger errorCount = new AtomicInteger(0);
                    private AtomicBoolean onNextCalled = new AtomicBoolean(false);
                    
                    @Override
                    public void onCompleted() {
                        // ok to ignore
                    }

                    @Override
                    public void onError(Throwable e) {
                        if (requestCount.get() == 1 && e instanceof NoSuchElementException) {
                            s.onError(e);
                        }
                        
                        if (!retriableError.call(e) || errorCount.incrementAndGet() > 1) {
                            s.onError(e);
                        }
                    }

                    @Override
                    public void onNext(R t) {
                        if (onNextCalled.compareAndSet(false, true)) {
                            s.onNext(t);
                            s.onCompleted();
                        }
                    }
                };
                
                // Choose a host and forward errors and response to opObserver
                final Observer<C> lbObserver = new Observer<C>() {
                    @Override
                    public void onCompleted() {
                        // OK to ignore.  Should never return null.
                    }

                    @Override
                    public void onError(Throwable e) {
                        opObserver.onError(e);
                    }

                    @Override
                    public void onNext(C t) {
                        cs.add(operation.call(t).subscribe(opObserver));
                    }
                };
                
                // This is the initial request
                cs.add(lb.choose().subscribe(lbObserver));
                
                // This is the backup request
                cs.add(scheduler.createWorker().schedule(new Action0() {
                    @Override
                    public void call() {
                        cs.add(lb.choose().subscribe(lbObserver));
                    }
                }, delay.call(), units));
                
                s.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        cs.unsubscribe();
                    }
                }));
            }
        });
    }

    public static <C> BackupRequestExecutionStrategy<C> create(DefaultLoadBalancer<C> lb, Func0<Integer> delay) {
        return new BackupRequestExecutionStrategy<C>(lb, delay);
    }
}
