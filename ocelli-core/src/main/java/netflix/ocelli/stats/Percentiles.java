package netflix.ocelli.stats;

/**
 * Contract for tracking percentiles in a dataset
 * 
 * @author elandau
 */
public interface Percentiles {
    /**
     * Add a sample
     * @param value
     */
    public void add(int value);
    
    /**
     * @param percentile (0 .. 1.0)
     * @return Get the Nth percentile
     */
    public int percentile(double percentile);
    
    /**
     * @return maximum sample value
     */
    public int max();
    
    /**
     * @return minimum sample value
     */
    public int min();
    
    /**
     * @return Get the sample mean
     */
    public int mean();
    
    /**
     * @return Return the median (or 50th percentile)
     */
    public int median();
}
