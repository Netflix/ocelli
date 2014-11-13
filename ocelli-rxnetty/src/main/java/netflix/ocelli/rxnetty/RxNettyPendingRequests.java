package netflix.ocelli.rxnetty;

import rx.functions.Func1;

/**
 * A function to extract pending requests metric from {@link MetricAwareClientHolder} for HTTP clients.
 */
public class RxNettyPendingRequests<I, O> implements Func1<HttpClientHolder<I, O>, Integer> {

    @Override
    public Integer call(HttpClientHolder holder) {
        return holder.getListener().getPendingRequests();
    }
}
