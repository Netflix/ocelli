package netflix.ocelli.metrics;

import netflix.ocelli.MetricsFactory;
import rx.functions.Action0;

public class SimpleClientMetricsFactory<Host> implements MetricsFactory<Host, ClientMetrics> {
    @Override
    public ClientMetrics call(Host t1, Action0 shutdown) {
        return new SimpleClientMetrics(shutdown);
    }
}
