package netflix.ocelli;

import java.util.NoSuchElementException;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;



/**
 * The LoadBalancer provides simple access to any load balancing algorithm
 * where the next best T is retrieved by calling next().  
 * 
 * There are no guarantees that calling next consecutively for retries will return a 
 * different T
 * 
 * @author elandau
 * 
 * @param <T>
 */
public abstract class LoadBalancer<T> {
    public abstract T next() throws NoSuchElementException;
    public abstract void shutdown();
    
    public Observable<T> toObservable() {
        return Observable.create(new OnSubscribe<T>() {
            @Override
            public void call(Subscriber<? super T> s) {
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
