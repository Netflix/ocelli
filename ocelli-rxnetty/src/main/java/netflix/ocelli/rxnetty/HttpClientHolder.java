package netflix.ocelli.rxnetty;

import io.reactivex.netty.metrics.HttpClientMetricEventsListener;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.CloseableInstance;
import netflix.ocelli.Instance;
import netflix.ocelli.util.SingleMetric;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.BehaviorSubject;

/**
 * An extension of {@link MetricAwareClientHolder} for HTTP.
 *
 * @author Nitesh Kant
 */
public class HttpClientHolder<I, O> extends MetricAwareClientHolder<HttpClientRequest<I>, HttpClientResponse<O>, HttpClient<I, O>, HttpMetricListener> {
    
    private AtomicReference<BehaviorSubject<Void>> lifecycle = new AtomicReference<BehaviorSubject<Void>>(BehaviorSubject.<Void>create());
    
    public HttpClientHolder(HttpClient<I, O> client, SingleMetric<Long> metric) {
        super(client, new HttpMetricListener(metric));
        
        client.subscribe(new HttpClientMetricEventsListener() {
            @Override
            protected void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
                lifecycle.get().onCompleted();
            }
        });
    }

    @Override
    public HttpMetricListener getListener() {
        return super.getListener();
    }
    
    public static <I, O> Func1<HttpClientHolder<I, O>, Observable<Instance<HttpClientHolder<I, O>>>> factory() {
        return new Func1<HttpClientHolder<I, O>, Observable<Instance<HttpClientHolder<I, O>>>>() {
            @Override
            public Observable<Instance<HttpClientHolder<I, O>>> call(final HttpClientHolder<I, O> client) {
                return Observable
                    .defer(new Func0<Observable<Instance<HttpClientHolder<I, O>>>>() {
                        @Override
                        public Observable<Instance<HttpClientHolder<I, O>>> call() {
                            BehaviorSubject<Void> subject = BehaviorSubject.<Void>create();
                            client.lifecycle.set(subject);
                            return Observable.<Instance<HttpClientHolder<I, O>>>just(CloseableInstance.from(client, subject));
                        }
                    })
                    .delaySubscription(1, TimeUnit.SECONDS);
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

    public void fail() {
        lifecycle.get().onCompleted();
    }
}
