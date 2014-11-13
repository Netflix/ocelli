package netflix.ocelli.rxnetty;

import io.reactivex.netty.client.ClientMetricsEvent;
import io.reactivex.netty.client.RxClient;
import io.reactivex.netty.metrics.MetricEventsListener;
import netflix.ocelli.LoadBalancer;

/**
 * A composite for any {@link RxClient} and set of metrics required by {@link LoadBalancer}.
 *
 * @author Nitesh Kant
 */
public class MetricAwareClientHolder<I, O, T extends RxClient<I, O>, L extends MetricEventsListener<ClientMetricsEvent<?>>> {

    private final T client;
    private final L listener;

    public MetricAwareClientHolder(T client, L listener) {
        this.client = client;
        this.listener = listener;
        this.client.subscribe(listener);
    }

    public T getClient() {
        return client;
    }

    public L getListener() {
        return listener;
    }
}
