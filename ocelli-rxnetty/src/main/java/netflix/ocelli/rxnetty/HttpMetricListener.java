package netflix.ocelli.rxnetty;

import io.reactivex.netty.client.RxClient;
import io.reactivex.netty.metrics.HttpClientMetricEventsListener;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.util.SingleMetric;

/**
 * An {@link RxClient} metric listener to calculate metrics for {@link LoadBalancer}
 *
 * @author Nitesh Kant
 */
public class HttpMetricListener extends HttpClientMetricEventsListener {

    private final AtomicInteger pendingRequests = new AtomicInteger();
    private final AtomicInteger attemptsSinceLastFail = new AtomicInteger();
    
    private final SingleMetric<Long> metric;
    
    public HttpMetricListener(SingleMetric<Long> metric) {
        this.metric = metric;
    }

    public int getPendingRequests() {
        return pendingRequests.get();
    }

    public Long getMetric() {
        return metric.get();
    }
    
    public int getAttemptsSinceLastFail() {
        return this.attemptsSinceLastFail.get();
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
        metric.add(TimeUnit.MILLISECONDS.convert(duration, timeUnit));
    }
}
