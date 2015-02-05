package netflix.ocelli.functions;

import rx.Observable;
import rx.functions.Func1;

public abstract class Connectors {
    public static <C> Func1<C, Observable<C>> never() {
        return new Func1<C, Observable<C>>() {
            @Override
            public Observable<C> call(C client) {
                return Observable.never();
            }
        };
    }
    
    public static <C> Func1<C, Observable<C>> immediate() {
        return new Func1<C, Observable<C>>() {
            @Override
            public Observable<C> call(C client) {
                return Observable.just(client);
            }
        };
    }
    
    public static <C> Func1<C, Observable<C>> failure(final Throwable t) {
        return new Func1<C, Observable<C>>() {
            @Override
            public Observable<C> call(C client) {
                return Observable.error(t);
            }
        };
    }

}
