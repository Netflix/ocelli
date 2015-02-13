package netflix.ocelli.functions;

import rx.Observable;
import rx.functions.Func1;

public abstract class Connectors {
    public static <C> Func1<C, Observable<Void>> never() {
        return new Func1<C, Observable<Void>>() {
            @Override
            public Observable<Void> call(C client) {
                return Observable.never();
            }
        };
    }
    
    public static <C> Func1<C, Observable<Void>> immediate() {
        return new Func1<C, Observable<Void>>() {
            @Override
            public Observable<Void> call(C client) {
                return Observable.empty();
            }
        };
    }
    
    public static <C> Func1<C, Observable<Void>> failure(final Throwable t) {
        return new Func1<C, Observable<Void>>() {
            @Override
            public Observable<Void> call(C client) {
                return Observable.error(t);
            }
        };
    }

}
