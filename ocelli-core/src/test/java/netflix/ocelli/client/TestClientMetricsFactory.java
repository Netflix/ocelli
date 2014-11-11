package netflix.ocelli.client;

import com.google.common.collect.Maps;

import netflix.ocelli.MetricsFactory;
import rx.functions.Action0;

import java.util.concurrent.ConcurrentMap;

public class TestClientMetricsFactory<Host> implements MetricsFactory<Host> {
    private final ConcurrentMap<Host, TestClientMetrics> instances = Maps.newConcurrentMap();
    
    @Override
    public TestClientMetrics call(Host t1, Action0 shutdown) {
        TestClientMetrics metrics = new TestClientMetrics(shutdown);
        instances.put(t1, metrics);
        return metrics;
    }
    
    public TestClientMetrics get(Host host) {
        return instances.get(host);
    }

    @Override
    public Class<?> getType() {
        return TestClientMetrics.class;
    }
    
}
