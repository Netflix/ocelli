package netflix.ocelli.execute;

import netflix.ocelli.LoadBalancer;
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

    private LoadBalancer<C> chooser;

    public SimpleExecutionStrategy(final LoadBalancer<C> lb) {
        this.chooser = lb;
    }
    
    @Override
    public <R> Observable<R> execute(final Func1<C, Observable<R>> operation) {
        return chooser.flatMap(operation);
    }

    public static <C> SimpleExecutionStrategy<C> create(LoadBalancer<C> chooser) {
        return new SimpleExecutionStrategy<C>(chooser);
    }
    
}
