package netflix.ocelli;

import rx.Observable;
import rx.Subscriber;

/**
 * Representation for an active instance of a client in the load balancer.
 * The client is an Observable<Boolean> associated with a failure detector
 * that emits true if the instance is up or false if down.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class Instance<T> extends Observable<Boolean> {
    
    public static <T> Instance<T> from(T value, Observable<Boolean> events) {
        return new Instance<T>(value, events);
    }
    
    private final T value;
    
    public Instance(T value, final Observable<Boolean> events) {
        super(new OnSubscribe<Boolean>() {
            @Override
            public void call(Subscriber<? super Boolean> t1) {
                events.subscribe(t1);
            }
        });
        
        this.value = value;
    }
    
    public T getValue() {
        return this.value;
    }
    
    public String toString() {
        return "Instance[" + value + "]";
    }
}
