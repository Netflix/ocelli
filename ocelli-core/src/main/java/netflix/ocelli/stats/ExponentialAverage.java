package netflix.ocelli.stats;

import com.google.common.util.concurrent.AtomicDouble;

public class ExponentialAverage implements Average {

    private final double k;
    private final AtomicDouble ema;
    private final double initial;
    
    public ExponentialAverage(int N, double initial) {
        this.initial = initial;
        this.k = 2.0/(double)(N+1);
        this.ema = new AtomicDouble(initial);
    }

    @Override
    public void add(int sample) {
        double next;
        double current;
        do {    
            current = ema.get();
            next = sample * k + current * (1-k);
        } while(!ema.compareAndSet(current, next));
    }

    @Override
    public double get() {
        return ema.get();
    }
    
    @Override
    public void reset() {
        ema.set(initial);
    }

}
