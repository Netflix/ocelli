package netflix.ocelli.rxnetty;

import io.reactivex.netty.metrics.MetricEventsListener;
import io.reactivex.netty.metrics.MetricsEvent;
import netflix.ocelli.Host;
import netflix.ocelli.Instance;

public interface HttpInstance<M extends MetricsEvent<?>> 
       extends 
            MetricEventsListener<M>, 
            Instance<Host> {
}
