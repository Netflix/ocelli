package netflix.ocelli.rxnetty;

import io.reactivex.netty.client.RxClient;
import io.reactivex.netty.metrics.HttpClientMetricEventsListener;
import netflix.ocelli.LoadBalancer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link RxClient} metric listener to calculate metrics for {@link LoadBalancer}
 *
 * @author Nitesh Kant
 */
public class HttpMetricListener extends HttpClientMetricEventsListener {

    public AtomicInteger pendingRequests = new AtomicInteger();

    public int getPendingRequests() {
        return pendingRequests.get();
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

}
