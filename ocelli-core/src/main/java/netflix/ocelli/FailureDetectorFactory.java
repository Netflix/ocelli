package netflix.ocelli;

import rx.Observable;
import rx.functions.Func1;

/**
 * Contract for a failure detector.  A subscription to the failure detector
 * is made of each client instance in a load balancer.  The returned Observable
 * will emit an exception for each failure. A subscription is likely to persist
 * for the entire lifetime of the client.
 * 
 * @author elandau
 *
 * @param <C>
 */
public interface FailureDetectorFactory<C> extends Func1<C, Observable<Throwable>>{
    @Override
    public Observable<Throwable> call(C client);
}
