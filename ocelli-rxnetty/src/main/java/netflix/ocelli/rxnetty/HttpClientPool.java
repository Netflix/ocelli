package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurator;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import netflix.ocelli.Host;
import rx.functions.Func1;

/**
 * An implementation of {@link RxNettyClientPool} for HTTP clients.
 *
 * @author Nitesh Kant
 */
public class HttpClientPool<I, O> extends RxNettyClientPool<HttpClientRequest<I>, HttpClientResponse<O>, HttpClient<I, O>> {

    private final Func1<Host, HttpClient<I, O>> clientFactory;

    public HttpClientPool(final PipelineConfigurator<HttpClientResponse<O>, HttpClientRequest<I>> configurator) {
        this(new Func1<Host, HttpClient<I, O>>() {
            @Override
            public HttpClient<I, O> call(Host host) {
                return RxNetty.createHttpClient(host.getHostName(), host.getPort(), configurator);
            }
        });
    }

    public HttpClientPool(Func1<Host, HttpClient<I, O>> clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    protected HttpClient<I, O> createClient(Host host) {
        return clientFactory.call(host);
    }

    public static HttpClientPool<ByteBuf, ByteBuf> newPool() {
        return new HttpClientPool<ByteBuf, ByteBuf>(new Func1<Host, HttpClient<ByteBuf, ByteBuf>>() {
            @Override
            public HttpClient<ByteBuf, ByteBuf> call(Host host) {
                return RxNetty.createHttpClient(host.getHostName(), host.getPort());
            }
        });
    }
}
