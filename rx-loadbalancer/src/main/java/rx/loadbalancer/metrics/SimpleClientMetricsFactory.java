package rx.loadbalancer.metrics;

import rx.functions.Action0;
import rx.loadbalancer.FailureDetector;
import rx.loadbalancer.FailureDetectorFactory;

public class SimpleClientMetricsFactory<Host> implements FailureDetectorFactory<Host> {
    @Override
    public FailureDetector call(Host t1, Action0 shutdown) {
        return new SimpleClientMetrics(shutdown);
    }
}
