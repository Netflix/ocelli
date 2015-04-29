package netflix.ocelli;

import rx.Observable;

/**
 * Representation of a single instance within a pool. 
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
