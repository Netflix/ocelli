package netflix.ocelli;

import rx.Observable;

public interface LoadBalancer<C> {

    /**
     * Return an Observable that when subscribed to will select the next C
     * in the pool.  Call chose() for each use of the load balancer as the Observable
     * will contain some context for selecting the next C on retries.
     * 
     * @return Observable that when subscribed to will emit a single C and complete
     */
    Observable<C> choose();
}
