package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

public class Requests {
    public static Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>> from(HttpClientRequest<ByteBuf> request) {
        return new Func1<HttpClientHolder<ByteBuf, ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
            @Override
            public Observable<HttpClientResponse<ByteBuf>> call(HttpClientHolder<ByteBuf, ByteBuf> holder) {
                return holder.getClient()
                             .submit(HttpClientRequest.createGet("/"))
                             .map(new Func1<HttpClientResponse<ByteBuf>, HttpClientResponse<ByteBuf>>() {
                                 @Override
                                 public HttpClientResponse<ByteBuf> call(HttpClientResponse<ByteBuf> response) {
                                     response.ignoreContent();
                                     return response;
                                 }
                             });
            }
        };
    }
}
