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
import netflix.ocelli.util.SingleMetric;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;

public class HttpInstanceImpl extends HttpClientMetricEventsListener implements HttpInstance<ClientMetricsEvent<?>> {

    public static Func1<HttpInstanceImpl, Observable<HttpInstanceImpl>> connector() {
        return new Func1<HttpInstanceImpl, Observable<HttpInstanceImpl>>() {
            @Override
            public Observable<HttpInstanceImpl> call(HttpInstanceImpl i) {
                i = new HttpInstanceImpl(i);
                Observable<HttpInstanceImpl> o = Observable.just(i);

                long delay = i.metricsListener.getAttemptsSinceLastFail();;
                if (delay > 0) {
                    o = o.delaySubscription(delay, TimeUnit.SECONDS);
                }
                
                return o;
            }
        };
    }
    
    public static Func1<HttpInstanceImpl, Instance<HttpClient<ByteBuf, ByteBuf>>> toClient() {
        return new Func1<HttpInstanceImpl, Instance<HttpClient<ByteBuf, ByteBuf>>>() {
            @Override
            public Instance<HttpClient<ByteBuf, ByteBuf>> call(final HttpInstanceImpl server) {
                final HttpClient<ByteBuf, ByteBuf> client = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(server.getValue().getHostName(), server.getValue().getPort())
                    .channelOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 100)
                    .build();

                final Subscription sub = client.subscribe(server);
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
     * Lifecycle control for this instance
     */
    private final BehaviorSubject<Void> control = BehaviorSubject.create();
    
    /**
     * Final lifecycle for this instance.  For all incarnations of the Server this is an 
     * AMB of the primary server and the failure detected lifecycle
     */
    private final Observable<Void>      lifecycle;
    
    private final int                   incarnationId;
    
    public HttpInstanceImpl(Host host, SingleMetric<Long> metric, Observable<Void> lifecycle) {
        this(new HttpMetricListener(metric), host, lifecycle);
    }
    
    public HttpInstanceImpl(HttpMetricListener state, Host host, Observable<Void> lifecycle) {
        this.metricsListener = state;
        this.host            = host;
        this.lifecycle       = lifecycle;
        this.incarnationId   = 0;
    }

    HttpInstanceImpl(HttpInstanceImpl server) {
        this.metricsListener = server.metricsListener;
        this.host            = server.host;
        this.lifecycle       = server.lifecycle.ambWith(control).cache();
        this.incarnationId   = metricsListener.incIncarnation();
    }

    @Override
    public Host getValue() {
        return host;
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
        control.onCompleted();
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
}
