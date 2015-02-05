package netflix.ocelli;

import rx.subjects.PublishSubject;

/**
 * Member of a host/client pool that can be shut down or manually
 * as an indication that it is no longer a member of the pool.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class CloseableMember<T> extends Member<T> {

    public static <T> CloseableMember<T> from(T value) {
        return new CloseableMember<T>(value, PublishSubject.<Void>create());
    }

    private final PublishSubject<Void> signal;
    
    public CloseableMember(T value, PublishSubject<Void> signal) {
        super(value, signal);
        this.signal = signal;
    }

    /**
     * Indicate that the Member is no longer in the pool by sending an onCompleted
     * on the underlying subject.  Any subscriber to this Member object will
     * have it's onCompleted invoked.
     */
    public void close() {
        this.signal.onCompleted();
    }
}
