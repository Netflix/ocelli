package netflix.ocelli;

import rx.Observable;

/**
 * Representation of a single instance within a pool. 
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
