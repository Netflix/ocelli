package netflix.ocelli.rxnetty;

import io.reactivex.netty.client.ClientMetricsEvent;
import io.reactivex.netty.metrics.HttpClientMetricEventsListener;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.Host;
import netflix.ocelli.rxnetty.ServerPool.Server;
import netflix.ocelli.util.SingleMetric;
import rx.Observable;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;

public class HttpServerImpl implements Server<ClientMetricsEvent<?>>  {

    private HttpMetricListener          state;
    private Host                        host;
    
    private final BehaviorSubject<Void> control = BehaviorSubject.create();
    private final Observable<Void>      lifecycle;
    
    private HttpClientMetricEventsListener listener;
    
    public HttpServerImpl(Host host, SingleMetric<Long> metric, Observable<Void> lifecycle) {
        this(new HttpMetricListener(metric), host, lifecycle);
    }
    
    public HttpServerImpl(HttpMetricListener state, Host host, Observable<Void> lifecycle) {
        this.state     = state;
        this.host      = host;
        this.lifecycle = lifecycle;
    }

    HttpServerImpl(HttpServerImpl server) {
        this.state     = server.state;
        this.host      = server.host;
        this.lifecycle = server.lifecycle.ambWith(control).cache();
        
        this.listener = new HttpClientMetricEventsListener() {
            @Override
            protected void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
                control.onCompleted();
            }
        };
    }

    @Override
    public Host getHost() {
        return host;
    }

    @Override
    public Observable<Void> getLifecycle() {
        return lifecycle;
    }
    
    @Override
    public Server<ClientMetricsEvent<?>> getValue() {
        return this;
    }
    
    public static Func1<HttpServerImpl, Observable<HttpServerImpl>> failureDetector() {
        return new Func1<HttpServerImpl, Observable<HttpServerImpl>>() {
            @Override
            public Observable<HttpServerImpl> call(HttpServerImpl i) {
                i = new HttpServerImpl(i);
                Observable<HttpServerImpl> o = Observable.just(i);

                long delay = i.state.getAttemptsSinceLastFail();;
                if (delay > 0) {
                    o = o.delaySubscription(delay, TimeUnit.SECONDS);
                }
                
                return o;
            }
        };
    }
    
    public String toString() {
        return "HttpServerImpl[" + host + "]";
    }

    @Override
    public void onEvent(ClientMetricsEvent<?> event, long duration,
            TimeUnit timeUnit, Throwable throwable, Object value) {
        state.onEvent(event, duration, timeUnit, throwable, value);
        listener.onEvent(event, duration, timeUnit, throwable, value);
    }
    
    @Override
    public void onCompleted() {
        state.onCompleted();
        listener.onCompleted();
    }

    @Override
    public void onSubscribe() {
        state.onSubscribe();
        listener.onSubscribe();
    }


}
