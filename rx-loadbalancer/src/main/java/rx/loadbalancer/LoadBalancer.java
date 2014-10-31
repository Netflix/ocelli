package rx.loadbalancer;

import rx.Observable;

/**
 * A concrete ClientSelector keeps track of all available hosts and returns 
 * the most recent immutable collection of connected {@link Client}'s.
 * 
 * @author elandau
 */
public interface LoadBalancer<Host, Client> {
    /**
     * Return an Observable that when subscribed to will select the next Client
     * in the pool.  Call select() for each use of the load balancer.  The Observable
     * will contain some context for selecting the next Client.
     * 
     * @return
     */
    Observable<Client> select();

    /**
     * @return Stream of all host events
     */
    Observable<HostEvent<Host>> events();
}
