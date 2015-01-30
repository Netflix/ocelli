package netflix.ocelli.stats;

import netflix.ocelli.SingleMetric;
import netflix.ocelli.util.AtomicDouble;
import rx.functions.Func0;

public class ExponentialAverage implements SingleMetric<Long> {

    private final double k;
    private final AtomicDouble ema;
    private final double initial;
    
    public static Func0<SingleMetric<Long>> factory(final int N, final double initial) {
        return new Func0<SingleMetric<Long>>() {
            @Override
            public SingleMetric<Long> call() {
                return new ExponentialAverage(N, initial);
            }
        };
    }
    
    public ExponentialAverage(int N, double initial) {
        this.initial = initial;
        this.k = 2.0/(double)(N+1);
        this.ema = new AtomicDouble(initial);
    }

    @Override
    public void add(Long sample) {
        double next;
        double current;
        do {    
            current = ema.get();
            next = sample * k + current * (1-k);
        } while(!ema.compareAndSet(current, next));
    }

    @Override
    public Long get() {
        return (long)ema.get();
    }
    
    @Override
    public void reset() {
        ema.set(initial);
    }

}
