package netflix.ocelli.execute;

import netflix.ocelli.LoadBalancer;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * Very simple implementation of invoker that simply executes the operations 
 * without any additional retry or failover logic
 * 
 * @author elandau
 *
 * @param <C>
 */
public class SimpleExecutionStrategy<C, I, O> implements ExecutionStrategy<I, O> {

    private Observable<C> lb;
    private final Func2<C, I, Observable<O>> operation;
    
    public static <C, I, O> Func2<LoadBalancer<C>, Func2<C, I, Observable<O>>, ExecutionStrategy<I, O>> factory() {
        return new Func2<LoadBalancer<C>, Func2<C, I, Observable<O>>, ExecutionStrategy<I, O>>() {
            @Override
            public ExecutionStrategy<I, O> call(LoadBalancer<C> t1, Func2<C, I, Observable<O>> t2) {
                return new SimpleExecutionStrategy<C, I, O>(t1, t2);
            }
        };
    }
    public SimpleExecutionStrategy(final LoadBalancer<C> lb, final Func2<C, I, Observable<O>> operation) {
        this.lb = Observable.create(lb);
        this.operation = operation;
    }

    @Override
    public Observable<O> call(final I request) {
        return lb.concatMap(new Func1<C, Observable<O>>() {
            @Override
            public Observable<O> call(C client) {
                return operation.call(client, request);
            }
        });
    }
    
    public static <C, I, O> SimpleExecutionStrategy<C, I, O> create(LoadBalancer<C> lb, final Func2<C, I, Observable<O>> operation) {
        return new SimpleExecutionStrategy<C, I, O>(lb, operation);
    }
    
}
