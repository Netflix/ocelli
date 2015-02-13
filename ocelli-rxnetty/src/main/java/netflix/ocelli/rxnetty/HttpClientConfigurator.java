package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.executor.ExecutorBuilder;
import netflix.ocelli.executor.ExecutorBuilder.Configurator;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Metrics;
import netflix.ocelli.util.SingleMetric;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

public class HttpClientConfigurator implements Configurator<Host, HttpClientHolder<ByteBuf, ByteBuf>, HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> {

    private final Observable<Instance<Host>> hosts;
    
    public HttpClientConfigurator(Observable<Instance<Host>> hosts) {
        this.hosts = hosts;
    }
    
    @Override
    public void configure(ExecutorBuilder<Host, HttpClientHolder<ByteBuf, ByteBuf>, HttpClientRequest<ByteBuf>, HttpClientResponse<ByteBuf>> builder) {
        final Func0<SingleMetric<Long>> metricFactory = new Func0<SingleMetric<Long>>() {
            @Override
            public SingleMetric<Long> call() {
                return Metrics.memoize(10L);
            }
        };
        
        builder
            .withInstances(hosts)
            .withClientFactory(
                new Func1<Host, HttpClientHolder<ByteBuf, ByteBuf>>() {
                    @Override
                    public HttpClientHolder<ByteBuf, ByteBuf> call(Host host) {
                        return new HttpClientHolder<ByteBuf, ByteBuf>(
                                RxNetty.createHttpClient(host.getHostName(), host.getPort()),
                                metricFactory.call());
                    }
                })
            .withQuarantineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
            .withFailureDetector(HttpClientHolder.<ByteBuf, ByteBuf>failureDetector())
            .withClientShutdown(HttpClientHolder.<ByteBuf, ByteBuf>shutdown())
            .withRequestOperation(new Func2<HttpClientHolder<ByteBuf, ByteBuf>, HttpClientRequest<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                @Override
                public Observable<HttpClientResponse<ByteBuf>> call(
                        HttpClientHolder<ByteBuf, ByteBuf> holder,
                        HttpClientRequest<ByteBuf> request) {
                    return holder.getClient().submit(request);
                }
            });
    }  
}
