package netflix.ocelli.client;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.metrics.ClientMetricsListener;
import rx.functions.Action0;

public class TestClientMetrics implements ClientMetricsListener {
    private Action0 shutdown;

    public TestClientMetrics(Action0 shutdown) {
        this.shutdown = shutdown;
    }
    
    public void shutdown() {
        this.shutdown.call();
    }

    @Override
    public void onEvent(ClientEvent event, long duration, TimeUnit timeUnit, Throwable throwable, Object value) {
    }
}
