package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.HostAddress;
import netflix.ocelli.metrics.ClientMetricsListener;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;

public class RxNettyHttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(RxNettyHttpClient.class);
    private final HttpClient<ByteBuf, ByteBuf> client;
    private final HostAddress server;
    private final ClientMetricsListener events;
    
    public RxNettyHttpClient(HttpClient<ByteBuf, ByteBuf> client, HostAddress server, ClientMetricsListener events, Observable<Void> signal) {
        this.client = client;
        this.server = server;
        this.events = events;
    }

    public Observable<HttpClientResponse<ByteBuf>> createGet(String uri) {
        HttpClientRequest<ByteBuf> request = HttpClientRequest.createGet(uri);
        
        return client.submit(request)
            .lift(new Operator<HttpClientResponse<ByteBuf>, HttpClientResponse<ByteBuf>>() {
                @Override
                public Subscriber<? super HttpClientResponse<ByteBuf>> call(final Subscriber<? super HttpClientResponse<ByteBuf>> sub) {
                    final long start = System.nanoTime();
                    events.onEvent(ClientEvent.REQUEST_START, 0, null, null, null); 
                    return new Subscriber<HttpClientResponse<ByteBuf>>(sub) {
                        @Override
                        public void onCompleted() {
                            sub.onCompleted();
                        }

                        @Override
                        public void onError(Throwable e) {
                            LOG.info("Error writing to server '{}' {}", server, e.getMessage());
                            final long end = System.nanoTime();
                            events.onEvent(ClientEvent.REQUEST_FAILURE, end - start, TimeUnit.NANOSECONDS, e, null);
                            sub.onError(e);
                        }

                        @Override
                        public void onNext(HttpClientResponse<ByteBuf> t) {
                            final long end = System.nanoTime();
                            events.onEvent(ClientEvent.REQUEST_SUCCESS, end - start, TimeUnit.NANOSECONDS, null, t);
                            sub.onNext(t);
                        }
                    };
                }
            })
            ;
    }
    
    public HttpClient<ByteBuf, ByteBuf> getClient() {
        return client;
    }

    public HostAddress getServer() {
        return server;
    }
    
}
