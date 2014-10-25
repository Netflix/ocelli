package rx.loadbalancer;

import rx.functions.Action1;

/**
 * Request and connection metrics associated with a Host
 * 
 * @author elandau
 *
 */
public interface FailureDetector extends Action1<ClientEvent> {
    long getConnectStartCount();
    long getConnectFailureCount();
    long getConnectSuccessCount();
    long getRequestStartCount();
    long getRequestFailureCount();
    long getRequestSuccessCount();
    int getWeight();
}
