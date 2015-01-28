package netflix.ocelli.execute;

import rx.Observable;
import rx.functions.Func1;

/**
 * The invoker encapsulates a specific usage of the load balancer and uses
 * ioc to execute an operation.  An invoker will normally capture specific
 * policies such as failover and retries.
 * 
 * @author elandau
 *
 * @param <C>
 */
public abstract class ExecutionStrategy<C> {

    /**
     * Execute the operation on the connection pool 
     * @param operation
     * @return Observable with the final response for the operation
     */
    public abstract <R> Observable<R> execute(Func1<C, Observable<R>> operation);
    
    
    /**
     * Converts the invoker into a mapping function that can be composed into 
     * a stream of request functions
     * @return
     */
    public <R> Func1<Func1<C, Observable<R>>, Observable<R>> asFunction() {
        return new Func1<Func1<C, Observable<R>>, Observable<R>>() {
            @Override
            public Observable<R> call(Func1<C, Observable<R>> t1) {
                return execute(t1);
            }
        };
    }
}
