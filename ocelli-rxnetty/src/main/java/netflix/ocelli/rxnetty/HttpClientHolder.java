package netflix.ocelli.rxnetty;

import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import netflix.ocelli.SingleMetric;
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

}
