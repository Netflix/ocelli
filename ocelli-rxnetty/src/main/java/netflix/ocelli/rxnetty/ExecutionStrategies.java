package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.executor.ExecutorBuilder;
import rx.Observable;

public abstract class ExecutionStrategies {
    public static ExecutorBuilder<Host, HttpClientHolder<ByteBuf, ByteBuf>, HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> newHttpClient(Observable<Instance<Host>> hosts) {
        ExecutorBuilder<Host, HttpClientHolder<ByteBuf, ByteBuf>, HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> builder = ExecutorBuilder.builder();
        new HttpClientConfigurator(hosts).configure(builder);
        return builder;
    }
}
