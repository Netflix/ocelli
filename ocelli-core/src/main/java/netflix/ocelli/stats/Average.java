package netflix.ocelli.stats;

/**
 * Contract for calculating an average
 * 
 * @author elandau
 *
 */
public interface Average {
    /**
     * Add a single sample
     * @param sample
     */
    void add(int sample);
    
    /**
     * @return  Current average
     */
    double get();

    /**
     * Reset the average to the initial value
     */
    void reset();
}
