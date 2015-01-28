package netflix.ocelli.util;

public class ExpAvg implements Average {
    
    private final double k;
    private volatile double ema = 0;
    
    public ExpAvg(int N) {
        this.k = 2.0/(double)(N+1);
    }
    
    @Override
    public synchronized void addSample(long sample) {
        ema = sample * k + ema * (1-k);
    }

    @Override
    public double get() {
        return ema;
    }

    @Override
    public synchronized void reset() {
        ema = 0;
    }
}
