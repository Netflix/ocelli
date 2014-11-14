package netflix.ocelli.retrys;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;

/**
 * Func1 to be passed to retryWhen which implements a robust random exponential backoff.  The
 * random part of the exponential backoff ensures that some randomness is inserted so that multiple
 * clients blocked on a non-responsive resource spread out the retries to mitigate a thundering
 * herd.  
 * 
 * This class maintains retry count state and should be instantiated for entire top level request.
 * 
 * @author elandau
 */
public class ExponentialBackoff implements Func1<Observable<? extends Throwable>, Observable<?>> {
    private static final int MAX_SHIFT = 30;
    
    private final int maxRetrys;
    private final long maxDelay;
    private final long slice;
    private final TimeUnit units;
    private final Func1<Throwable, Boolean> retryable;

    private static final Random rand = new Random();
    
    private int tryCount;
    
    /**
     * Construct an exponential backoff 
     * 
     * @param maxRetrys Maximum number of retires to attempt
     * @param slice - Time interval multiplied by backoff amount
     * @param maxDelay - Upper bound allowable backoff delay
     * @param units - Time unit for slice and maxDelay
     * @param retryable - Function that returns true if the error is retryable or false if not.  
     */
    public ExponentialBackoff(int maxRetrys, long slice, long maxDelay, TimeUnit units, Func1<Throwable, Boolean> retryable) {
        this.maxDelay = maxDelay;
        this.maxRetrys = maxRetrys;
        this.slice = slice;
        this.units = units;
        this.retryable = retryable;
        
        this.tryCount = 0;
    }
    
    @Override
    public Observable<?> call(Observable<? extends Throwable> error) {
        return error.flatMap(new Func1<Throwable, Observable<?>>() {
            @Override
            public Observable<?> call(Throwable e) {
                // First make sure the error is actually retryable
                if (!retryable.call(e)) {
                    return Observable.error(e);
                }
                
                if (tryCount >= maxRetrys) {
                    return Observable.error(new Exception("Failed with " + tryCount + " retries", e));
                }
                
                // Calculate the number of slices to wait
                int slices = (1 << Math.min(MAX_SHIFT, tryCount));
                slices = (slices + rand.nextInt(slices+1)) / 2;
                long delay = slices * slice;
                if (maxDelay > 0 && delay > maxDelay) {
                    delay = maxDelay;
                }
                tryCount++;
                return Observable.timer(delay, units);
            }
        });
    }
}
