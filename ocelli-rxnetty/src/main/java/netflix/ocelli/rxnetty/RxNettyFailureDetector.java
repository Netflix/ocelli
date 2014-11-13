package netflix.ocelli.rxnetty;

import io.reactivex.netty.metrics.HttpClientMetricEventsListener;
import netflix.ocelli.FailureDetectorFactory;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

import java.util.concurrent.TimeUnit;

/**
 * A failure detector for RxNetty that detects failures which determine host health.
 */
public class RxNettyFailureDetector<I, O> implements FailureDetectorFactory<HttpClientHolder<I, O>>{

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
