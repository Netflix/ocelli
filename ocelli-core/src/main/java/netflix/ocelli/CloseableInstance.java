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
public abstract class CloseableInstance<T> extends Instance<T> {
    
    public static <T> CloseableInstance<T> from(T value) {
        return from(value, BehaviorSubject.<Void>create());
    }
    
    public static <T> CloseableInstance<T> from(final T value, final BehaviorSubject<Void> events) {
        return new CloseableInstance<T>() {
            @Override
            public void close() {
                events.onCompleted();
            }

            @Override
            public Observable<Void> getLifecycle() {
                return events;
            }

            @Override
            public T getValue() {
                return value;
            }
        };
    }
    
    public static <T> Func1<T, CloseableInstance<T>> toMember() {
        return new Func1<T, CloseableInstance<T>>() {
            @Override
            public CloseableInstance<T> call(T t) {
                return from(t);
            }
        };
    }

    public String toString() {
        return "CloseableInstance[" + getValue() + "]";
    }
    
    public abstract void close();
}
