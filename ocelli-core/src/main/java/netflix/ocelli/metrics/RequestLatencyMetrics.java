package netflix.ocelli.metrics;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.metrics.math.ExpAvg;

public class RequestLatencyMetrics implements ClientMetricsListener {
    private ExpAvg longAvg = new ExpAvg(20);
    private ExpAvg shortAvg = new ExpAvg(20);
    
    public long getLatencyScore() {
        return (long)longAvg.get();
    }

    @Override
    public void onEvent(ClientEvent event, long duration, TimeUnit timeUnit, Throwable throwable, Object value) {
        switch (event) {
        case REQUEST_SUCCESS:
            long dur = TimeUnit.MILLISECONDS.convert(duration, timeUnit);
            longAvg.addSample(dur);
            shortAvg.addSample(dur);
            break;
        default:
            break;
        }
    }
}
