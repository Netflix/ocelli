package netflix.ocelli.rxnetty;

import io.reactivex.netty.metrics.HttpClientMetricEventsListener;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func1;

/**
 * A failure detector for RxNetty that detects failures which determine host health.
 */
public class RxNettyFailureDetector<I, O> implements Func1<HttpClientHolder<I, O>, Observable<Throwable>>{

    @Override
    public Observable<Throwable> call(final HttpClientHolder<I, O> holder) {
        return Observable.create(new OnSubscribe<Throwable>() {
            @Override
            public void call(final Subscriber<? super Throwable> sub) {
                holder.getClient().subscribe(new HttpClientMetricEventsListener() {
                    @Override
                    protected void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
                        sub.onNext(throwable);
                    }
                });
            }
        });
    }
}
