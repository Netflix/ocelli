package netflix.ocelli.stats;

/**
 * Contract for tracking percentiles in a dataset
 * 
 * @author elandau
 */
public interface Quantiles {
    /**
     * Add a sample
     * @param value
     */
    public void insert(int value);
    
    /**
     * @param get (0 .. 1.0)
     * @return Get the Nth percentile
     */
    public int get(double percentile);
}
