package netflix.ocelli.functions;

import rx.Observable;
import rx.functions.Func1;

public abstract class Failures {
    public static <C> Func1<C, Observable<Throwable>> never() {
        return new Func1<C, Observable<Throwable>>() {
            @Override
            public Observable<Throwable> call(C client) {
                return Observable.never();
            }
        };
    }
    
    public static <C> Func1<C, Observable<Throwable>> always(final Throwable t) {
        return new Func1<C, Observable<Throwable>>() {
            @Override
            public Observable<Throwable> call(C client) {
                return Observable.error(t);
            }
        };
    }
}
