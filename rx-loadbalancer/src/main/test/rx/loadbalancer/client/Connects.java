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
    
    public static Observable<Void> failure(final long timeout, final TimeUnit units, final Exception error) {
        return Observable.create(new OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> t1) {
                try {
                    units.sleep(timeout);
                    t1.onError(error);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    t1.onError(e);
                }
            }
        });
    }
}
