package netflix.ocelli.execute;

import java.util.List;

import rx.Observable;
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
public class InterceptingRequestExecutionStrategy<C, I, O> implements ExecutionStrategy<I, O> {

    public static interface Interceptor<I, O> {
        I pre(I request);
        O post(O response);
    }
    
    private final ExecutionStrategy<I, O> delegate;
    private final List<Interceptor<I, O>> interceptors;
    
    public InterceptingRequestExecutionStrategy(ExecutionStrategy<I, O> delegate, List<Interceptor<I, O>> interceptors) {
        this.delegate = delegate;
        this.interceptors = interceptors;
    }
    
    @Override
    public Observable<O> call(I request) {
        for (Interceptor<I, O> interceptor : interceptors) {
            request = interceptor.pre(request);
        }

        return delegate.call(request)
            .map(new Func1<O, O>() {
                @Override
                public O call(O response) {
                    for (Interceptor<I, O> interceptor : interceptors) {
                        response = interceptor.post(response);
                    }
                    return response;
                }
            });
    }
}
