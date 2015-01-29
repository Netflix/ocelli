package netflix.ocelli;

import rx.Observable;
import rx.Observable.OnSubscribe;

/**
 * Base for all LoadBalancers will emit a single C based on the load balancing
 * strategy when subscribed to.
 * 
 * @author elandau
 *
 * @param <C>
 */
public abstract class LoadBalancer<C> implements OnSubscribe<C> {
    /**
     * Shut down the load balancer
     * 
     * TODO: Re-implement using ConnectableObservable
     */
    public abstract void shutdown();
    
    /**
     * @return  Observable that when subscribed to will emit all currently active
     *          clients in the load balancer
     */
    public abstract Observable<C> all();
}
