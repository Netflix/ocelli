package netflix.ocelli;

import java.util.concurrent.TimeUnit;

/**
 * A stopwatch starts counting when the object is created and can be used
 * to track how long operations take.  For simplicity this contract does
 * not provide a mechanism to stop, restart, or clear the stopwatch.  Instead
 * it just returns the elapsed time since the object was created.
 * 
 * @author elandau
 * @see {@link Stopwatches}
 */
public interface Stopwatch {
    /**
     * Elapsed time since object was created.
     * @param units
     * @return
     */
    long elapsed(TimeUnit units);
}
