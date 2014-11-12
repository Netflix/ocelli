package netflix.ocelli.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RpsEstimator {
    private static final double MICROS_PER_SECOND = TimeUnit.MICROSECONDS.convert(1, TimeUnit.SECONDS);
    
    private AtomicLong sampleCounter = new AtomicLong();
    private AtomicLong lastCheckpoint = new AtomicLong();
    
    private volatile long estimatedRps;
    private volatile long nextCheckpoint;
    private volatile long lastFlushTime = System.nanoTime();

    public static class State {
        long count;
        long rps;
    }
    
    public RpsEstimator(long initialRps) {
        this.estimatedRps = 0;
        this.nextCheckpoint = 1000;
    }
    
    public State addSample() {
        long counter = sampleCounter.incrementAndGet();
        if (counter - lastCheckpoint.get() == nextCheckpoint) {
            long count = counter - lastCheckpoint.get();
            synchronized (this) {
                lastCheckpoint.set(counter);
                long now = System.nanoTime();
                estimatedRps = (long) (count * MICROS_PER_SECOND / (lastFlushTime - now));
                lastFlushTime = now;
                nextCheckpoint = estimatedRps;
                
                State state = new State();
                state.count = count;
                state.rps = estimatedRps;
                return state;
            }
        }
        else if (counter - lastCheckpoint.get() > 2 * estimatedRps) {
            nextCheckpoint = (counter - lastCheckpoint.get()) * 2;
        }
        
        return null;
    }
    
    long getSampleCount() {
        return sampleCounter.get();
    }
    
    long getRps() {
        return estimatedRps;
    }
}
