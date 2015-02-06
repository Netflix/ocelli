package netflix.ocelli.rxnetty;

import io.reactivex.netty.metrics.HttpClientMetricEventsListener;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.util.SingleMetric;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * An extension of {@link MetricAwareClientHolder} for HTTP.
 *
 * @author Nitesh Kant
 */
public class HttpClientHolder<I, O> extends MetricAwareClientHolder<HttpClientRequest<I>, HttpClientResponse<O>, HttpClient<I, O>, HttpMetricListener> {
    
    public HttpClientHolder(HttpClient<I, O> client, SingleMetric<Long> metric) {
        super(client, new HttpMetricListener(metric));
    }

    @Override
    public HttpMetricListener getListener() {
        return super.getListener();
    }
    
    public static <I, O> Func1<HttpClientHolder<I, O>, Observable<Throwable>> failureDetector() {
        return new Func1<HttpClientHolder<I, O>, Observable<Throwable>>() {
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
        };
    }

    /**
     * Comparison by pending request for load balancing decisions
     * @return
     */
    public static <I, O> Func2<HttpClientHolder<I, O>, HttpClientHolder<I, O>, HttpClientHolder<I, O>> byPendingRequest() {
        return new Func2<HttpClientHolder<I, O>, HttpClientHolder<I, O>, HttpClientHolder<I, O>>() {
            @Override
            public HttpClientHolder<I, O> call(
                    HttpClientHolder<I, O> t1,
                    HttpClientHolder<I, O> t2) {
                return t1.getListener().getPendingRequests() > t2.getListener().getPendingRequests() ?
                       t1 : t2;
            }
        };
    }

    /**
     * Comparison by average request latency for load balancing decisions
     * @return
     */
    public static <I, O> Func2<HttpClientHolder<I, O>, HttpClientHolder<I, O>, HttpClientHolder<I, O>> byAverageLatency() {
        return new Func2<HttpClientHolder<I, O>, HttpClientHolder<I, O>, HttpClientHolder<I, O>>() {
            @Override
            public HttpClientHolder<I, O> call(
                    HttpClientHolder<I, O> t1,
                    HttpClientHolder<I, O> t2) {
                return t1.getListener().getMetric() > t2.getListener().getMetric() ?
                       t1 : t2;
            }
        };
    }

    public static <I, O> Action1<HttpClientHolder<I, O>> shutdown() {
        return new Action1<HttpClientHolder<I, O>>() {
            @Override
            public void call(HttpClientHolder<I, O> t1) {
                t1.getClient().shutdown();
            }
        };
    }
}
