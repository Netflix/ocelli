package netflix.ocelli;

import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;

/**
 * Representation of an Instance that can be manually manipulated to set it's up/down
 * state as well as final completion (i.e. removal from the pool).
 * 
 * @author elandau
 *
 * @param <T>
 */
public class MutableInstance<T> extends Instance<T> {
    
    public static <T> MutableInstance<T> from(T value) {
        return new MutableInstance<T>(value, BehaviorSubject.<Boolean>create(true));
    }
    
    public static <T> MutableInstance<T> from(T value, BehaviorSubject<Boolean> subject) {
        return new MutableInstance<T>(value, subject);
    }
    
    public static <T> Func1<T, MutableInstance<T>> toMember() {
        return new Func1<T, MutableInstance<T>>() {
            @Override
            public MutableInstance<T> call(T t) {
                return from(t);
            }
        };
    }

    private final BehaviorSubject<Boolean> events;
    
    private MutableInstance(T value, final BehaviorSubject<Boolean> events) {
        super(value, new OnSubscribe<Boolean>() {
            @Override
            public void call(Subscriber<? super Boolean> s) {
                events.subscribe(s);
            }
        });
        this.events = events;
    }

    public void close() {
        events.onCompleted();
    }
    
    public void setState(boolean isUp) {
        events.onNext(isUp);
    }
}
