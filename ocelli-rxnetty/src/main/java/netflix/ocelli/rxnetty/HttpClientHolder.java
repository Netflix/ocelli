package netflix.ocelli.rxnetty;

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
    
    public static <I, O> Func1<HttpClient<I, O>, HttpClientHolder<I, O>> toHolder() {
        return new Func1<HttpClient<I, O>, HttpClientHolder<I, O>>() {
            @Override
            public HttpClientHolder<I, O> call(HttpClient<I, O> client) {
                return new HttpClientHolder<I, O>(client);
            }
        };
    }
    
    public HttpClientHolder(HttpClient<I, O> client) {
        super(client, new HttpMetricListener());
    }

    @Override
    public HttpMetricListener getListener() {
        return super.getListener();
    }
}
