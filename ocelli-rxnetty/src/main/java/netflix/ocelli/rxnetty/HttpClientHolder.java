package netflix.ocelli.rxnetty;

import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import netflix.ocelli.stats.Average;

/**
 * An extension of {@link MetricAwareClientHolder} for HTTP.
 *
 * @author Nitesh Kant
 */
public class HttpClientHolder<I, O> extends MetricAwareClientHolder<HttpClientRequest<I>, HttpClientResponse<O>, HttpClient<I, O>, HttpMetricListener> {
    
    public HttpClientHolder(HttpClient<I, O> client, Average average, PoolHttpMetricListener listener) {
        super(client, new HttpMetricListener(average, listener));
    }

    @Override
    public HttpMetricListener getListener() {
        return super.getListener();
    }
}
