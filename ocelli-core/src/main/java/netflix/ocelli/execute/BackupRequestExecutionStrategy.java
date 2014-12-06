package netflix.ocelli.execute;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

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
    
    private LoadBalancer<C> lb;
    private Func0<Integer> delay;

    public BackupRequestExecutionStrategy(final LoadBalancer<C> lb, final Func0<Integer> delay) {
        this.lb = lb;
        this.delay = delay;
    }
    
    @Override
    public <R> Observable<R> execute(final Func1<C, Observable<R>> operation) {
        return Observable.amb(
                lb.choose().flatMap(operation),
                Observable.timer(delay.call(), TimeUnit.MILLISECONDS).flatMap(new Func1<Long, Observable<R>>() {
                    @Override
                    public Observable<R> call(Long t1) {
                        return lb.choose().flatMap(operation);
                    }
                }));
    }

    public static <C> BackupRequestExecutionStrategy<C> create(DefaultLoadBalancer<C> lb, Func0<Integer> delay) {
        return new BackupRequestExecutionStrategy<C>(lb, delay);
    }
}
