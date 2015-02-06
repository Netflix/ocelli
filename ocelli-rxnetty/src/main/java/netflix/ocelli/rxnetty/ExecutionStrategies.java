package netflix.ocelli.rxnetty;

import rx.Observable;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import netflix.ocelli.Host;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.executor.ExecutorBuilder;

public abstract class ExecutionStrategies {
    public static ExecutorBuilder<Host, HttpClientHolder<ByteBuf, ByteBuf>, HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> newHttpClient(Observable<MembershipEvent<Host>> hosts) {
        ExecutorBuilder<Host, HttpClientHolder<ByteBuf, ByteBuf>, HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> builder = ExecutorBuilder.builder();
        new HttpClientConfigurator(hosts).configure(builder);
        return builder;
    }
}
