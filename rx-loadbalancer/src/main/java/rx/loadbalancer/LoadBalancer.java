package rx.loadbalancer;

import rx.Observable;

/**
 * SPI for a load balancer.  
 * 
 * @author elandau
 */
public interface LoadBalancer<Client> {
    /**
     * Return an Observable that when subscribed to will select the next Client
     * in the pool.  Call select() for each use of the load balancer.  The Observable
     * will contain some context for selecting the next Client.
     * 
     * @return
     */
    Observable<Client> select();
}
