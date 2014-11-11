package netflix.ocelli.metrics;

import netflix.ocelli.MetricsFactory;
import rx.functions.Action0;

public class CoreClientMetricsFactory<H> implements MetricsFactory<H> {
    @Override
    public ClientMetricsListener call(H t1, Action0 shutdown) {
        return new CoreClientMetrics(shutdown);
    }

    @Override
    public Class<?> getType() {
        return CoreClientMetrics.class;
    }
}
