package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;

public class RxNettyMetricsConnector implements Func1<HttpClient<ByteBuf, ByteBuf>, Observable<HttpClientMetrics>> {

    @Override
    public Observable<HttpClientMetrics> call(HttpClient<ByteBuf, ByteBuf> t1) {
        final HttpClientMetrics metrics = new HttpClientMetrics();
        final Subscription s = t1.subscribe(metrics);
        return Observable
                .just(metrics)
                .concatWith(Observable.<HttpClientMetrics>never())
                .doOnUnsubscribe(new Action0() {
                    @Override
                    public void call() {
                        s.unsubscribe();
                    }
                });
    }
}
