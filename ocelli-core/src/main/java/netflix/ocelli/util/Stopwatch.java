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
        return units.convert(getRawElapsed(), TimeUnit.NANOSECONDS);
    }
    
    public long getRawElapsed() {
        if (endTime == -1) {
            return System.nanoTime() - startTime;
        }
        return endTime - startTime;
    }
}
