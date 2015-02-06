package netflix.ocelli.executor;

import java.util.List;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * ExecutionStrategy decorator that provides per request pre and post processing hooks
 * 
 * @author elandau
 *
 * @param <C>
 * @param <I>
 * @param <O>
 */
public class InterceptingExecutor<I, O> implements Executor<I, O> {

    public static interface Interceptor<I, O> {
        I pre(I request);
        O post(I request, O response);
        void error(I request, Throwable error);
    }
    
    private final Executor<I, O> delegate;
    private final List<Interceptor<I, O>> interceptors;
    
    public InterceptingExecutor(Executor<I, O> delegate, List<Interceptor<I, O>> interceptors) {
        this.delegate = delegate;
        this.interceptors = interceptors;
    }
    
    @Override
    public Observable<O> call(I request) {
        for (Interceptor<I, O> interceptor : interceptors) {
            request = interceptor.pre(request);
        }

        return _call(request);
    }
    
    private Observable<O> _call(final I request) {
        return delegate
            .call(request)
            .doOnError(new Action1<Throwable>() {
                @Override
                public void call(Throwable error) {
                    for (Interceptor<I, O> interceptor : interceptors) {
                        interceptor.error(request, error);
                    }
                }
            })
            .map(new Func1<O, O>() {
                @Override
                public O call(O response) {
                    for (Interceptor<I, O> interceptor : interceptors) {
                        response = interceptor.post(request, response);
                    }
                    return response;
                }
            });
    }
}
