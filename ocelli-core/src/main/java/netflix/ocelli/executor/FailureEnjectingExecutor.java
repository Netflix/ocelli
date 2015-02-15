package netflix.ocelli.executor;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;

/**
 * Basic decorator to an Executor that can be used to inject failures.  
 * 
 * @author elandau
 *
 * @param <I>
 * @param <O>
 */
public class FailureEnjectingExecutor<I, O> implements Executor<I, O> {

    private final Executor<I, O> delegate;

    public FailureEnjectingExecutor(Executor<I, O> delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public Observable<O> call(I request) {
        Observable<O> o;
        
        Throwable error = getError();
        if (error != null) {
            o = Observable.error(error);
        }
        else {
            o = delegate.call(request);
        }
        
        Func1<O, O> transformer = getResponseTransformer();
        if (transformer != null) {
            o = o.map(transformer);
        }
        
        long delay = getDelay();
        if (delay > 0) {
            o = o.delaySubscription(delay, getDelayUnits());
        }
        
        return o;
    }
    
    /**
     * @return Function to modify a successful response
     */
    private Func1<O, O> getResponseTransformer() {
        return null;
    }

    /**
     * @return Get subscription delay to introduce.  Note that this is an absolute delay to however
     * long the operation may take.
     */
    protected long getDelay() {
        return 0;
    }
    
    /**
     * @return Units to use for the delay.  Default is MILLISECONDS
     */
    protected TimeUnit getDelayUnits() {
        return TimeUnit.MILLISECONDS;
    }
    
    /**
     * @return Get error to inject
     */
    protected Throwable getError() {
        return null;
    }
}
