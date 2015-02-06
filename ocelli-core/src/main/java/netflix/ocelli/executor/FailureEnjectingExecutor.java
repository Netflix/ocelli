package netflix.ocelli.executor;

import java.util.concurrent.TimeUnit;

import rx.Observable;

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
        Observable<O> otherResponse = getOtherResponse();
        
        Throwable error = getError();
        if (error != null) {
            o = delegate.call(request);
        }
        else if (otherResponse != null) {
            o = otherResponse;
        }
        else {
            o = Observable.error(error);
        }
        
        long delay = getDelay();
        if (delay > 0) {
            o = o.delaySubscription(delay, getDelayUnits());
        }
        
        return o;
    }
    
    /**
     * @return A different response Observable to be used instead of calling the delegate
     */
    private Observable<O> getOtherResponse() {
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
