package netflix.ocelli.metrics;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.ClientEvent;

/**
 * Contract for a listener to receive metrics from a client implementation and 
 * using these metrics to set stats for the client or to feed into a failure
 * detector.
 * 
 * @author elandau
 */
public interface ClientMetricsListener {
    void onEvent(ClientEvent event, long duration, TimeUnit timeUnit, Throwable throwable, Object value);
}
