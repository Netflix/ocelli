package netflix.ocelli.rxnetty;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.client.ClientMetricsEvent;
import io.reactivex.netty.metrics.MetricEventsListener;
import io.reactivex.netty.protocol.http.client.HttpClient;
import netflix.ocelli.FailureDetector;

public class RxNettyFailureDetector implements FailureDetector<HttpClient<ByteBuf, ByteBuf>>{

    @Override
    public Observable<Throwable> call(final HttpClient<ByteBuf, ByteBuf> client) {
        return Observable.create(new OnSubscribe<Throwable>() {
            @Override
            public void call(final Subscriber<? super Throwable> sub) {
                client.subscribe(new MetricEventsListener<ClientMetricsEvent<?>>() {
                    @Override
                    public void onEvent(ClientMetricsEvent<?> event,
                            long duration, TimeUnit timeUnit,
                            Throwable throwable, Object value) {
                        if (event.isError())
                            sub.onNext(throwable);
                    }

                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onSubscribe() {
                    }
                });
            }
        });
    }

}
