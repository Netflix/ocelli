package netflix.ocelli;

import rx.Observable;

/**
 * Base for all LoadBalancers will emit a single C based on the load balancing
 * strategy when subscribed to.
 * 
 * @author elandau
 *
 * @param <C>
 */
public abstract class LoadBalancer<C> extends Observable<C> {
    protected LoadBalancer(rx.Observable.OnSubscribe<C> onSubscribe) {
        super(onSubscribe);
    }
    
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
