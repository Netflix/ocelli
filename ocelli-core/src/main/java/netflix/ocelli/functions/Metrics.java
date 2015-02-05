package netflix.ocelli.functions;

import rx.functions.Func0;
import netflix.ocelli.stats.CKMSQuantiles;
import netflix.ocelli.stats.Quantiles;
import netflix.ocelli.util.SingleMetric;

/**
 * Utility class for creating common strategies for tracking specific types of metrics
 * 
 * @author elandau
 *
 */
public class Metrics {
    public static <T> Func0<SingleMetric<T>> memoizeFactory(final T value) {
        return new Func0<SingleMetric<T>>() {
            @Override
            public SingleMetric<T> call() {
                return memoize(value);
            }
        };
    }
    
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
    
    public static Func0<SingleMetric<Long>> quantileFactory(final double percentile) {
        return new Func0<SingleMetric<Long>>() {
            @Override
            public SingleMetric<Long> call() {
                return quantile(percentile);
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
