package netflix.ocelli.client;

import netflix.ocelli.metrics.SimpleClientMetrics;
import rx.functions.Action0;

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
