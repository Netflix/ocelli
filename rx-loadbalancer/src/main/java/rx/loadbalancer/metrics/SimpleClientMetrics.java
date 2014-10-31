package rx.loadbalancer.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import rx.functions.Action0;
import rx.loadbalancer.ClientEvent;
import rx.loadbalancer.metrics.math.ExpAvg;

public class SimpleClientMetrics implements ClientMetrics {
    private AtomicLong requestStartCount   = new AtomicLong();
    private AtomicLong requestFailureCount = new AtomicLong();
    private AtomicLong requestSuccessCount = new AtomicLong();
    private AtomicLong connectStartCount   = new AtomicLong();
    private AtomicLong connectSuccessCount = new AtomicLong();
    private AtomicLong connectFailureCount = new AtomicLong();
    
    private Action0 shutdown;
    private ExpAvg longAvg = new ExpAvg(20);
    private ExpAvg shortAvg = new ExpAvg(20);
    
    public SimpleClientMetrics(Action0 shutdown) {
        this.shutdown = shutdown;
    }
    
    @Override
    public void call(ClientEvent event) {
        switch (event.getType()) {
        case REMOVED:
            break;
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
            int dur = (int) event.getDuration(TimeUnit.MILLISECONDS);
            longAvg.addSample(dur);
            shortAvg.addSample(dur);
            break;
        case REQUEST_FAILURE:
            requestFailureCount.incrementAndGet();
            shutdown.call();
            break;
        }
    }

    @Override
    public long getConnectStartCount() {
        return connectStartCount.get();
    }

    @Override
    public long getConnectFailureCount() {
        return connectFailureCount.get();
    }

    @Override
    public long getConnectSuccessCount() {
        return connectSuccessCount.get();
    }

    @Override
    public long getRequestStartCount() {
        return requestStartCount.get();
    }

    @Override
    public long getRequestFailureCount() {
        return requestFailureCount.get();
    }

    @Override
    public long getRequestSuccessCount() {
        return requestSuccessCount.get();
    }
    
    @Override
    public long getPendingConnectCount() {
        return connectStartCount.get() - connectSuccessCount.get() - connectFailureCount.get();
    }
    
    @Override
    public long getPendingRequestCount() {
        return requestStartCount.get() - requestSuccessCount.get() - requestFailureCount.get();
    }

    @Override
    public long getLatencyScore() {
        return (long)longAvg.get();
    }
}
