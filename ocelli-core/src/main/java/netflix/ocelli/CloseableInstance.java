package netflix.ocelli;

import rx.Observable;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;

/**
 * An Instance that can be manually closed to indicate it is no longer
 * in existence and should be removed from the connection pool.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class CloseableInstance<T> extends Instance<T> {
    
    public static <T> CloseableInstance<T> from(T value) {
        return from(value, BehaviorSubject.<Void>create());
    }
    
    public static <T> CloseableInstance<T> from(final T value, final BehaviorSubject<Void> lifecycle) {
        return new CloseableInstance<T>(value, lifecycle);
    }
    
    public static <T> Func1<T, CloseableInstance<T>> toMember() {
        return new Func1<T, CloseableInstance<T>>() {
            @Override
            public CloseableInstance<T> call(T t) {
                return from(t);
            }
        };
    }

    private T value;
    private BehaviorSubject<Void> lifecycle;

    public CloseableInstance(T value, BehaviorSubject<Void> lifecycle) {
        this.value = value;
        this.lifecycle = lifecycle;
    }
    
    public String toString() {
        return "CloseableInstance[" + getValue() + "]";
    }
    
    /**
     * onComplete the instance's lifecycle Observable<Void>
     */
    public void close() {
        lifecycle.onCompleted();
    }

    @Override
    public Observable<Void> getLifecycle() {
        return lifecycle;
    }

    @Override
    public T getValue() {
        return value;
    }
}
