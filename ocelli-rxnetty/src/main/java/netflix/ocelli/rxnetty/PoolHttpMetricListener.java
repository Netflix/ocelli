package netflix.ocelli.rxnetty;

import io.reactivex.netty.metrics.HttpClientMetricEventsListener;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.stats.CKMSQuantiles;
import netflix.ocelli.stats.Quantiles;

/**
 * Track metrics for an entire pool
 * 
 * @author elandau
 *
 */
public class PoolHttpMetricListener extends HttpClientMetricEventsListener {
    private final Quantiles percentiles = new CKMSQuantiles(new CKMSQuantiles.Quantile[]{new CKMSQuantiles.Quantile(0.5, 1), new CKMSQuantiles.Quantile(0.90, 1)});
    
    public Integer getLatencyPercentile(double percentile) {
        return percentiles.get(percentile);
    }

    @Override
    protected void onRequestProcessingComplete(long duration, TimeUnit timeUnit) {
        percentiles.insert((int) TimeUnit.MILLISECONDS.convert(duration, timeUnit));
    }
}
