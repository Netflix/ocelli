package rx.loadbalancer.client;

import rx.functions.Action0;
import rx.loadbalancer.metrics.SimpleClientMetrics;

public class TestClientMetrics extends SimpleClientMetrics {
    private Action0 shutdown;

    public TestClientMetrics(Action0 shutdown) {
        super(shutdown);
        
        this.shutdown = shutdown;
    }
    
    public void shutdown() {
        this.shutdown.call();
    }
}
