package rx.loadbalancer.metrics;

import rx.functions.Action1;
import rx.loadbalancer.ClientEvent;

/**
 * The failure detector takes as input events for a Client and
 * performs an analysis based on the input to determine if the client
 * is down.  
 * 
 * An instaed of a FailureDetector is created for each Client by
 * the FailureDectorFactory.  A failure detector is associated
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
}
