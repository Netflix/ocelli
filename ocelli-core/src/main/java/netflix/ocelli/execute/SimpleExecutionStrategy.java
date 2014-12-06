package netflix.ocelli.execute;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import rx.Observable;
import rx.functions.Func1;

/**
 * Very simple implementation of invoker that simply executes the operations 
 * without any additional retry or failover logic
 * 
 * @author elandau
 *
 * @param <C>
 */
public class SimpleExecutionStrategy<C> extends ExecutionStrategy<C> {

    private LoadBalancer<C> lb;

    public SimpleExecutionStrategy(final LoadBalancer<C> lb) {
        this.lb = lb;
    }
    
    @Override
    public <R> Observable<R> execute(final Func1<C, Observable<R>> operation) {
        return lb
                .choose()
                .flatMap(operation);
    }

    public static <C> SimpleExecutionStrategy<C> create(DefaultLoadBalancer<C> lb) {
        return new SimpleExecutionStrategy<C>(lb);
    }
    
}
