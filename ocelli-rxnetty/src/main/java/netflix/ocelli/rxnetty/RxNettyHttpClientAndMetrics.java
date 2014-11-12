package netflix.ocelli.rxnetty;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.client.ClientMetricsEvent;
import io.reactivex.netty.metrics.MetricEventsListener;
import io.reactivex.netty.protocol.http.client.HttpClient;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Subscription;

public class RxNettyHttpClientAndMetrics implements MetricEventsListener<ClientMetricsEvent<?>> {

    private final AtomicInteger pendingRequests = new AtomicInteger();
    
    private final HttpClient<ByteBuf, ByteBuf> client;
    private final String hostAndPort;
    
    public RxNettyHttpClientAndMetrics(String hostPort, HttpClient<ByteBuf, ByteBuf> client) {
        this.hostAndPort = hostPort;
        this.client = client;
        
        final Subscription s = client.subscribe(this);
    }
    
    @Override
    public void onEvent(ClientMetricsEvent<?> event, long duration, TimeUnit timeUnit, Throwable throwable, Object value) {
        if (event.getType() instanceof ClientMetricsEvent.EventType) {
            switch ((ClientMetricsEvent.EventType) event.getType()) {
            case ConnectStart:
            case ConnectSuccess:
            case ConnectFailed:
            case ConnectionCloseStart:
            case ConnectionCloseSuccess:
            case ConnectionCloseFailed:
            case PoolAcquireStart:
                pendingRequests.incrementAndGet();
                break;
            case PoolAcquireSuccess:
                break;
            case PoolAcquireFailed:
                pendingRequests.decrementAndGet();
                break;
            case PooledConnectionReuse:
            case PooledConnectionEviction:
            case PoolReleaseStart:
                break;
            case PoolReleaseSuccess:
                pendingRequests.decrementAndGet();
                break;
            case PoolReleaseFailed:
                break;
            case WriteStart:
            case WriteSuccess:
            case WriteFailed:
            case FlushStart:
            case FlushSuccess:
            case FlushFailed:
            case BytesRead:
                break;
            }
        }
    }

    @Override
    public void onCompleted() {
    }

    @Override
    public void onSubscribe() {
    }

    public int getPendingRequests() {
        return pendingRequests.get();
    }
    
    public HttpClient<ByteBuf, ByteBuf> getClient() {
        return client;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((hostAndPort == null) ? 0 : hostAndPort.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RxNettyHttpClientAndMetrics other = (RxNettyHttpClientAndMetrics) obj;
        if (hostAndPort == null) {
            if (other.hostAndPort != null)
                return false;
        } else if (!hostAndPort.equals(other.hostAndPort))
            return false;
        return true;
    }
}
