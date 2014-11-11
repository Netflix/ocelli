package netflix.ocelli.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import netflix.ocelli.ClientEvent;
import rx.functions.Action0;

public class CoreClientMetrics implements ClientMetricsListener {
    private AtomicLong requestStartCount   = new AtomicLong();
    private AtomicLong requestFailureCount = new AtomicLong();
    private AtomicLong requestSuccessCount = new AtomicLong();
    private AtomicLong connectStartCount   = new AtomicLong();
    private AtomicLong connectSuccessCount = new AtomicLong();
    private AtomicLong connectFailureCount = new AtomicLong();
    
    private Action0 shutdown;
    
    public CoreClientMetrics(Action0 shutdown) {
        this.shutdown = shutdown;
    }
    
    public long getConnectStartCount() {
        return connectStartCount.get();
    }

    public long getConnectFailureCount() {
        return connectFailureCount.get();
    }

    public long getConnectSuccessCount() {
        return connectSuccessCount.get();
    }

    public long getRequestStartCount() {
        return requestStartCount.get();
    }

    public long getRequestFailureCount() {
        return requestFailureCount.get();
    }

    public long getRequestSuccessCount() {
        return requestSuccessCount.get();
    }
    
    public long getPendingConnectCount() {
        return connectStartCount.get() - connectSuccessCount.get() - connectFailureCount.get();
    }
    
    public long getPendingRequestCount() {
        return requestStartCount.get() - requestSuccessCount.get() - requestFailureCount.get();
    }

    @Override
    public void onEvent(ClientEvent event, long duration, TimeUnit timeUnit, Throwable throwable, Object value) {
        switch (event) {
        case CONNECT_START:
            connectStartCount.incrementAndGet();
            break;
        case CONNECT_SUCCESS:
            connectSuccessCount.incrementAndGet();
            break;
        case CONNECT_FAILURE:
            connectFailureCount.incrementAndGet();
            shutdown.call();
            break;
        case REQUEST_START:
            requestStartCount.incrementAndGet();
            break;
        case REQUEST_SUCCESS:
            requestSuccessCount.incrementAndGet();
            break;
        case REQUEST_FAILURE:
            requestFailureCount.incrementAndGet();
            shutdown.call();
            break;
        default:
            break;
        }
    }
}
