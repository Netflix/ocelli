package netflix.ocelli.util;

/**
 * Contract for tracking a single Metric.  For example, a SingleMetric may track an exponential moving
 * average where add() is called for each new sample and get() is called to get the current 
 * exponential moving average
 * 
 * @author elandau
 *
 * @param <T>
 */
public interface SingleMetric<T> {
    /**
     * Add a new sample
     * @param sample
     */
    void add(T sample);
    
    /**
     * Reset the value to default
     */
    void reset();
    
    /**
     * @return The latest calculated value
     */
    T get();
}
