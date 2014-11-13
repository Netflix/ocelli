package netflix.ocelli;

import rx.Observable;

/**
 * A concrete ClientSelector keeps track of all available hosts and returns 
 * the most recent immutable collection of connected {@link C}'s.
 * 
 * @author elandau
 */
public interface ManagedLoadBalancer<C> extends LoadBalancer<C> {
    /**
     * @return Observable of all hosts (active or not)
     */
    Observable<C> listAllClients();

    /**
     * @return All clients ready to serve traffic
     */
    Observable<C> listActiveClients();
}
