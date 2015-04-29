package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.client.ClientMetricsEvent;
import io.reactivex.netty.metrics.HttpClientMetricEventsListener;
import io.reactivex.netty.protocol.http.client.HttpClient;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceEvent;
import netflix.ocelli.InstanceEventListener;
import netflix.ocelli.InstanceQuarantiner.IncarnationFactory;
import netflix.ocelli.util.SingleMetric;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;

public class HttpInstanceImpl extends HttpClientMetricEventsListener implements HttpInstance<ClientMetricsEvent<?>> {

    public static IncarnationFactory<HttpInstanceImpl> connector() {
        return new IncarnationFactory<HttpInstanceImpl>() {
            @Override
            public HttpInstanceImpl create(HttpInstanceImpl value, InstanceEventListener listener, Observable<Void> lifecycle) {
                return new HttpInstanceImpl(value, listener, lifecycle);
            }
        };
    }
    
    public static Func1<Instance<HttpInstanceImpl>, Instance<HttpClient<ByteBuf, ByteBuf>>> toClient() {
        return new Func1<Instance<HttpInstanceImpl>, Instance<HttpClient<ByteBuf, ByteBuf>>>() {
            @Override
            public Instance<HttpClient<ByteBuf, ByteBuf>> call(final Instance<HttpInstanceImpl> server) {
                final HttpClient<ByteBuf, ByteBuf> client = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(
                        server.getValue().getHost().getHostName(), 
                        server.getValue().getHost().getPort())
                    .channelOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 100)
                    .build();

                final Subscription sub = client.subscribe(server.getValue());
                return new Instance<HttpClient<ByteBuf, ByteBuf>>() {
                    @Override
                    public Observable<Void> getLifecycle() {
                        return server.getLifecycle().doOnCompleted(new Action0() {
                            @Override
                            public void call() {
                                sub.unsubscribe();
                                client.shutdown();
                            }
                        });
                    }
    
                    @Override
                    public HttpClient<ByteBuf, ByteBuf> getValue() {
                        return client;
                    }
                };
            }
        };
    }
    
    /**
     * Shared metric state across all incarnations of the Server
     */
    private HttpMetricListener          metricsListener;
    
    /**
     * Shared host 
     */
    private Host                        host;
    
    /**
     * Final lifecycle for this instance.  
     */
    private final Observable<Void>      lifecycle;
    
    private final int                   incarnationId;
    
    private final InstanceEventListener listener;
    
    public HttpInstanceImpl(Host host, SingleMetric<Long> metric, Observable<Void> lifecycle) {
        this.metricsListener = new HttpMetricListener(metric);
        this.host            = host;
        this.lifecycle       = lifecycle;
        this.incarnationId   = 0;
        this.listener        = null;
    }

    HttpInstanceImpl(HttpInstanceImpl server, InstanceEventListener listener, Observable<Void> lifecycle) {
        this.metricsListener = server.metricsListener;
        this.host            = server.host;
        
        this.lifecycle       = lifecycle;
        this.listener        = listener;
        this.incarnationId   = metricsListener.incIncarnation();
    }

    @Override
    public Observable<Void> getLifecycle() {
        return lifecycle;
    }
    
    @Override
    public void onEvent(ClientMetricsEvent<?> event, long duration, TimeUnit timeUnit, Throwable throwable, Object value) {
        metricsListener.onEvent(event, duration, timeUnit, throwable, value);
        super.onEvent(event, duration, timeUnit, throwable, value);
    }
    
    @Override
    public void onCompleted() {
        metricsListener.onCompleted();
    }

    @Override
    public void onSubscribe() {
        metricsListener.onSubscribe();
    }

    @Override
    protected void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        listener.onEvent(InstanceEvent.EXECUTION_FAILED, duration, timeUnit, throwable, null);
    }
    
    @Override
    protected void onResponseHeadersReceived(long duration, TimeUnit timeUnit) {
        listener.onEvent(InstanceEvent.EXECUTION_SUCCESS, duration, timeUnit, null, null);
    }

    public HttpMetricListener getMetricListener() {
        return metricsListener;
    }
    
    public int getIncarnationId() {
        return this.incarnationId;
    }

    public String toString() {
        return "HttpServerImpl[" + host + "]";
    }

    @Override
    public Host getHost() {
        return host;
    }

    public static Func1<HttpInstanceImpl, Host> byHost() {
        return new Func1<HttpInstanceImpl, Host>() {
            @Override
            public Host call(HttpInstanceImpl t1) {
                return t1.getHost();
            }
        };
    }
}
