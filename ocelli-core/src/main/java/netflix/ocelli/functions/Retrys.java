package netflix.ocelli.functions;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.retrys.ExponentialBackoff;
import rx.Observable;
import rx.functions.Func1;

public abstract class Retrys {
    public static Func1<Throwable, Boolean> ALWAYS = new Func1<Throwable, Boolean>() {
        @Override
        public Boolean call(Throwable t1) {
            return true;
        }
    };
    
    public static Func1<Throwable, Boolean> NEVER = new Func1<Throwable, Boolean>() {
        @Override
        public Boolean call(Throwable t1) {
            return false;
        }
    };
    
    /**
     * Exponential backoff 
     * @param maxRetrys
     * @param timeslice
     * @param units
     * @return
     */
    public static Func1<Observable<? extends Throwable>, Observable<?>> exponentialBackoff(final int maxRetrys, final long timeslice, final TimeUnit units) {
        return new ExponentialBackoff(maxRetrys, timeslice, -1, units, ALWAYS);
    }

    /**
     * Bounded exponential backoff
     * @param maxRetrys
     * @param timeslice
     * @param units
     * @return
     */
    public static Func1<Observable<? extends Throwable>, Observable<?>> exponentialBackoff(final int maxRetrys, final long timeslice, final TimeUnit units, final long maxDelay) {
        return new ExponentialBackoff(maxRetrys, timeslice, maxDelay, units, ALWAYS);
    }
}
