package netflix.ocelli.failures;

import netflix.ocelli.FailureDetector;
import rx.Observable;

public class Failures {
    public static <C> FailureDetector<C> never() {
        return new FailureDetector<C>() {
            @Override
            public Observable<Throwable> call(C client) {
                return Observable.never();
            }
        };
    }
    
    public static <C> FailureDetector<C> always(final Throwable t) {
        return new FailureDetector<C>() {
            @Override
            public Observable<Throwable> call(C client) {
                return Observable.error(t);
            }
        };
    }
}
