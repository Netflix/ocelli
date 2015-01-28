package netflix.ocelli.rxnetty;

import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

/**
 * An extension of {@link MetricAwareClientHolder} for HTTP.
 *
 * @author Nitesh Kant
 */
public class HttpClientHolder<I, O> extends MetricAwareClientHolder<HttpClientRequest<I>, HttpClientResponse<O>, HttpClient<I, O>, HttpMetricListener> {

    public HttpClientHolder(HttpClient<I, O> client) {
        super(client, new HttpMetricListener());
    }

    @Override
    public HttpMetricListener getListener() {
        return super.getListener();
    }
}
