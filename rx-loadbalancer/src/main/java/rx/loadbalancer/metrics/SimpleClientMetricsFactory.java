package rx.loadbalancer.metrics;

import rx.functions.Action0;
import rx.loadbalancer.ClientTrackerFactory;

public class SimpleClientMetricsFactory<Host> implements ClientTrackerFactory<Host, ClientMetrics> {
    @Override
    public ClientMetrics call(Host t1, Action0 shutdown) {
        return new SimpleClientMetrics(shutdown);
    }
}
