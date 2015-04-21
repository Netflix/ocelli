package netflix.ocelli.rxnetty;

import io.reactivex.netty.metrics.MetricEventsListener;
import io.reactivex.netty.metrics.MetricsEvent;
import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.rxnetty.ServerPool.Server;

public abstract class ServerPool<M extends MetricsEvent<?>> extends LoadBalancer<Server<M>> {
    
    public static interface Server<M extends MetricsEvent<?>> extends 
            MetricEventsListener<M>, 
            Instance<Server<M>> {

        Host getHost();
    }
}
