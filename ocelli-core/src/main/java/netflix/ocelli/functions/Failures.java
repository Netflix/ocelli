package netflix.ocelli.functions;

import netflix.ocelli.FailureDetectorFactory;
import rx.Observable;

public class Failures {
    public static <C> FailureDetectorFactory<C> never() {
        return new FailureDetectorFactory<C>() {
            @Override
            public Observable<Throwable> call(C client) {
                return Observable.never();
            }
        };
    }
    
    public static <C> FailureDetectorFactory<C> always(final Throwable t) {
        return new FailureDetectorFactory<C>() {
            @Override
            public Observable<Throwable> call(C client) {
                return Observable.error(t);
            }
        };
    }
}
