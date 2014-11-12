package netflix.ocelli.rxnetty;

import io.reactivex.netty.client.ClientMetricsEvent;
import io.reactivex.netty.metrics.MetricEventsListener;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpClientMetrics implements MetricEventsListener<ClientMetricsEvent<?>> {

    private final AtomicInteger pendingRequests = new AtomicInteger();
    
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
}
