package netflix.ocelli;

import java.util.NoSuchElementException;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

/**
 * The LoadBalancer contract is similar to a Subject in that it receives (and caches) input
 * in the form of a List of active clients and emits a single client from that list based 
 * on the load balancing strategy for each subscription.
 * 
 * @author elandau
 *
 * @param <T>
 */
public abstract class LoadBalancer<T> {
    public abstract T next() throws NoSuchElementException;
    
    public Observable<T> toObservable() {
        return Observable.create(new OnSubscribe<T>() {
            @Override
            public void call(final Subscriber<? super T> s) {
                try {
                    s.onNext(next());
                    s.onCompleted();
                }
                catch (Exception e) {
                    s.onError(e);
                }
            }
        });
    }
}
