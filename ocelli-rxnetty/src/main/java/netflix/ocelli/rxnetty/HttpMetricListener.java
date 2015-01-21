package netflix.ocelli.rxnetty;

import io.reactivex.netty.client.ClientMetricsEvent;
import io.reactivex.netty.client.RxClient;
import io.reactivex.netty.metrics.ClientMetricEventsListener;
import io.reactivex.netty.metrics.HttpClientMetricEventsListener;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.stats.Average;

/**
 * An {@link RxClient} metric listener to calculate metrics for {@link LoadBalancer}
 *
 * @author Nitesh Kant
 */
public class HttpMetricListener extends HttpClientMetricEventsListener {

    private final AtomicInteger pendingRequests = new AtomicInteger();
    private final Average average;
    private final ClientMetricEventsListener<ClientMetricsEvent<?>> poolListener;
    
    public HttpMetricListener(Average average, ClientMetricEventsListener<ClientMetricsEvent<?>> poolListener) {
        this.poolListener = poolListener;
        this.average = average;
    }

    public int getPendingRequests() {
        return pendingRequests.get();
    }

    public int getAverageLatency() {
        return (int)average.get();
    }
    
    public void onEvent(ClientMetricsEvent<?> event, long duration, TimeUnit timeUnit, Throwable throwable, Object value) {
        super.onEvent(event, duration, timeUnit, throwable, value);
        poolListener.onEvent(event, duration, timeUnit, throwable, value);
    }

    @Override
    protected void onResponseReceiveComplete(long duration, TimeUnit timeUnit) {
        pendingRequests.decrementAndGet();
    }

    @Override
    protected void onResponseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingRequests.decrementAndGet();
    }

    @Override
    protected void onRequestWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingRequests.decrementAndGet();
    }

    @Override
    protected void onRequestSubmitted() {
        pendingRequests.incrementAndGet();
    }
    
    @Override
    protected void onRequestProcessingComplete(long duration, TimeUnit timeUnit) {
        average.add((int) TimeUnit.MILLISECONDS.convert(duration, timeUnit));
    }
}
