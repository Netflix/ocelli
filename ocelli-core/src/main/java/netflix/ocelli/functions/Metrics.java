package netflix.ocelli.functions;

import netflix.ocelli.SingleMetric;
import netflix.ocelli.stats.CKMSQuantiles;
import netflix.ocelli.stats.Quantiles;

/**
 * Utility class for creating common strategies for tracking specific types of metrics
 * 
 * @author elandau
 *
 */
public class Metrics {
    /**
     * Return a predetermine constant value regardless of samples added.
     * @param value
     * @return
     */
    public static <T> SingleMetric<T> memoize(final T value) {
        return new SingleMetric<T>() {
            @Override
            public void add(T sample) {
            }

            @Override
            public void reset() {
            }

            @Override
            public T get() {
                return value;
            }
        };
    }
    
    /**
     * Use the default CKMSQuantiles algorithm to track a specific percentile
     * @param percentile
     * @return
     */
    public static SingleMetric<Long> quantile(final double percentile) {
        return quantile(new CKMSQuantiles(new CKMSQuantiles.Quantile[]{new CKMSQuantiles.Quantile(percentile, 1)}), percentile);
    }
    
    /**
     * Use an externally provided Quantiles algorithm to track a single percentile.  Note that
     * quantiles may be shared and should track homogeneous operations.
     * 
     * @param quantiles
     * @param percentile
     * @return
     */
    public static SingleMetric<Long> quantile(final Quantiles quantiles, final double percentile) {
        return new SingleMetric<Long>() {
            @Override
            public void add(Long sample) {
                quantiles.insert(sample.intValue());
            }

            @Override
            public void reset() {
            }

            @Override
            public Long get() {
                return (long)quantiles.get(percentile);
            }
        };
    }
}
