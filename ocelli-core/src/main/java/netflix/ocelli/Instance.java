package netflix.ocelli;

import rx.Observable;

/**
 * Representation of a single instance within a pool.  Up/Down state is managed via
 * an Observable<Boolean> where emitting true means the member is active and false
 * means the member is not active (possible due to failure detection).  onCompleted
 * indicates that the PoolMember has been removed.
 * 
 * @author elandau
 *
 * @param <T>
 */
public interface Instance<T> {
    /**
     * Return the lifecycle for this object
     * @return
     */
    Observable<Void> getLifecycle();

    /**
     * Return the instance object which could be an address or an actual client implementation
     * @return
     */
    T getValue();
}
