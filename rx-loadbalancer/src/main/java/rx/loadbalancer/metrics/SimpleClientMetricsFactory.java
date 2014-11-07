package rx.loadbalancer.metrics;

import rx.functions.Action0;
import rx.loadbalancer.MetricsFactory;

public class SimpleClientMetricsFactory<Host> implements MetricsFactory<Host, ClientMetrics> {
    @Override
    public ClientMetrics call(Host t1, Action0 shutdown) {
        return new SimpleClientMetrics(shutdown);
    }
}
