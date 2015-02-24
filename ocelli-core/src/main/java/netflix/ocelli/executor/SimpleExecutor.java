package netflix.ocelli.executor;

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
public class SimpleExecutor<C, I, O> implements Executor<I, O> {

    private Observable<C> lb;
    private final Func2<C, I, Observable<O>> operation;
    
    public static <C, I, O> Func2<LoadBalancer<C>, Func2<C, I, Observable<O>>, Executor<I, O>> factory() {
        return new Func2<LoadBalancer<C>, Func2<C, I, Observable<O>>, Executor<I, O>>() {
            @Override
            public Executor<I, O> call(LoadBalancer<C> t1, Func2<C, I, Observable<O>> t2) {
                return new SimpleExecutor<C, I, O>(t1, t2);
            }
        };
    }
    public SimpleExecutor(final LoadBalancer<C> lb, final Func2<C, I, Observable<O>> operation) {
        this.lb = lb;
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
    
    public static <C, I, O> SimpleExecutor<C, I, O> create(LoadBalancer<C> lb, final Func2<C, I, Observable<O>> operation) {
        return new SimpleExecutor<C, I, O>(lb, operation);
    }
    
}
