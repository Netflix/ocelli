package netflix.ocelli.functions;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.Stopwatch;
import rx.functions.Func0;

/**
 * Utility class to create common Stopwatch factories in the form of Func0<Stopwatch>
 * functions
 * 
 * @author elandau
 *
 */
public class Stopwatches {
    /**
     * Stopwatch that calls System.nanoTime()
     * 
     * @return
     */
    public static Func0<Stopwatch> systemNano() {
        return new Func0<Stopwatch>() {
            @Override
            public Stopwatch call() {
                return new Stopwatch() {
                    private final long startTime = System.nanoTime();
                    
                    @Override
                    public long elapsed(TimeUnit units) {
                        return units.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                    }
                };
            }
        };
    }
}
