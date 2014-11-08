package netflix.ocelli.rxnetty;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.ClientEvent;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;

public class RxNettyHttpClient {
    private final HttpClient<ByteBuf, ByteBuf> client;
    private final ServerInfo server;
    private Action1<ClientEvent> events;
    
    public RxNettyHttpClient(HttpClient<ByteBuf, ByteBuf> client, ServerInfo server, Action1<ClientEvent> events, Observable<Void> signal) {
        this.client = client;
        this.server = server;
        this.events = events;
    }

    public Observable<HttpClientResponse<ByteBuf>> createGet(String uri) {
        return RxNetty
            .createHttpGet("http://" + server.getHost() + ":" + server.getPort() + uri)
            .lift(new Operator<HttpClientResponse<ByteBuf>, HttpClientResponse<ByteBuf>>() {
                @Override
                public Subscriber<? super HttpClientResponse<ByteBuf>> call(final Subscriber<? super HttpClientResponse<ByteBuf>> sub) {
                    final long start = System.nanoTime();
                    events.call(ClientEvent.requestStart());
                    return new Subscriber<HttpClientResponse<ByteBuf>>(sub) {
                        @Override
                        public void onCompleted() {
                            sub.onCompleted();
                        }

                        @Override
                        public void onError(Throwable e) {
                            final long end = System.nanoTime();
                            events.call(ClientEvent.requestFailure(end - start, TimeUnit.NANOSECONDS, e));
                            sub.onError(e);
                        }

                        @Override
                        public void onNext(HttpClientResponse<ByteBuf> t) {
                            final long end = System.nanoTime();
                            events.call(ClientEvent.requestSuccess(end - start, TimeUnit.NANOSECONDS));
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

    public ServerInfo getServer() {
        return server;
    }
    
}
