package rx.loadbalancer.client;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;

public class Connects {
    public static Observable<Void> delay(final long timeout, final TimeUnit units) {
        return Observable.timer(timeout, units).ignoreElements().cast(Void.class);
    }
    
    public static Observable<Void> failure() {
        return Observable.error(new Exception("Connectus interruptus"));
    }
    
    public static Observable<Void> failure(final long timeout, final TimeUnit units) {
        return Observable.timer(timeout, units).concatMap(new Func1<Long, Observable<Void>>() {
            @Override
            public Observable<Void> call(Long t1) {
                return Observable.error(new Exception("Connectus interruptus"));
            }
        });
    }

    public static Observable<Void> immediate() {
        return Observable.empty();
    }
    
    public static Observable<Void> never() {
        return Observable.never();
    }
    
    public static Observable<Void> error(Exception e) {
        return Observable.error(e);
    }
}
