package netflix.ocelli.metrics;

import netflix.ocelli.ClientEvent;
import rx.functions.Action1;

/**
 * The failure detector takes as input events for a Client and
 * performs an analysis based on the input to determine if the client
 * is down.  
 * 
 * An instance of ClientMetrics is created for each Client by
 * the ClientMetricsFactory.  A failure detector is associated
 * with the Client as well as a single Action0 callback to invoke
 * when the client has been determined to be 'down'.
 * 
 * @author elandau
 *
 */
public interface ClientMetrics extends Action1<ClientEvent> {
    long getConnectStartCount();
    long getConnectFailureCount();
    long getConnectSuccessCount();
    long getRequestStartCount();
    long getRequestFailureCount();
    long getRequestSuccessCount();
    long getPendingConnectCount();
    long getPendingRequestCount();
    long getLatencyScore();
}
