package rx.loadbalancer.client;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

public class Connects {
    public static Observable<Void> delay(final long timeout, final TimeUnit units) {
        return Observable.create(new OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> t1) {
                try {
                    units.sleep(timeout);
                    t1.onCompleted();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    t1.onError(e);
                }
            }
        });
    }
    
    public static Observable<Void> failure() {
        return Observable.error(new Exception("Connectus interruptus"));
    }
    
    public static Observable<Void> failure(final long timeout, final TimeUnit units) {
        return Observable.create(new OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> t1) {
                try {
                    units.sleep(timeout);
                    t1.onError(new Exception("Connectus interruptus"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    t1.onError(e);
                }
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
