package netflix.ocelli.rxnetty;

import netflix.ocelli.stats.Average;
import rx.functions.Func0;
import rx.functions.Func1;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

/**
 * An extension of {@link MetricAwareClientHolder} for HTTP.
 *
 * @author Nitesh Kant
 */
public class HttpClientHolder<I, O> extends MetricAwareClientHolder<HttpClientRequest<I>, HttpClientResponse<O>, HttpClient<I, O>, HttpMetricListener> {
    
    public static <I, O> Func1<HttpClient<I, O>, HttpClientHolder<I, O>> toHolder(final Func0<Average> averageFactory, final PoolHttpMetricListener listener) {
        return new Func1<HttpClient<I, O>, HttpClientHolder<I, O>>() {
            @Override
            public HttpClientHolder<I, O> call(HttpClient<I, O> client) {
                return new HttpClientHolder<I, O>(client, averageFactory.call(), listener);
            }
        };
    }
    
    public HttpClientHolder(HttpClient<I, O> client, Average average, PoolHttpMetricListener listener) {
        super(client, new HttpMetricListener(average, listener));
    }

    @Override
    public HttpMetricListener getListener() {
        return super.getListener();
    }
}
