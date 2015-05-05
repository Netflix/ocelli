package netflix.ocelli;

import rx.Observable;

/**
 * An {@link Instance} encapsulates a generic entity as well as its lifecycle.  Lifecycle
 * is managed as an Observable<Void> that onCompletes when the entity is no longer in the 
 * pool.  This technique is also used to introduce topologies and quarantine logic for any
 * client type where each incarnation of the entity within the load balancer has its own
 * lifecycle. 
 * 
 * Instance is used internally in Ocelli and should not be created directly other than 
 * for implementing specific entity registry solutions, such as Eureka.  
 * 
 * @see LoadBalancer
 * 
 * @author elandau
 *
 * @param <T>
 */
public abstract class Instance<T> {
    public static <T> Instance<T> create(final T value, final Observable<Void> lifecycle) {
        return new Instance<T>() {
            @Override
            public Observable<Void> getLifecycle() {
                return lifecycle;
            }

            @Override
            public T getValue() {
                return value;
            }
        };
    }
    
    /**
     * Return the lifecycle for this object
     * @return
     */
    public abstract Observable<Void> getLifecycle();

    /**
     * Return the instance object which could be an address or an actual client implementation
     * @return
     */
    public abstract T getValue();
    
    public String toString() {
        return "Instance[" + getValue() + "]";
    }
}
