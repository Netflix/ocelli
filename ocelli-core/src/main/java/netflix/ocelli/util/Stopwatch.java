package netflix.ocelli.util;

import java.util.concurrent.TimeUnit;

public class Stopwatch {

    public static Stopwatch createStarted() {
        return new Stopwatch(System.nanoTime());
    }
    
    private Stopwatch(long startTime) {
        this.startTime = startTime;
    }
    
    private long startTime;
    private long endTime = -1;
    
    public long elapsed(TimeUnit units) {
        if (endTime == -1) {
            return units.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
        return units.convert(endTime - startTime, TimeUnit.NANOSECONDS);
    }
}
