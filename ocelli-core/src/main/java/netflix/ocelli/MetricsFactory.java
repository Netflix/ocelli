package netflix.ocelli;

import netflix.ocelli.metrics.ClientMetricsListener;
import rx.functions.Action0;
import rx.functions.Func2;

/**
 * Factory for creating client instance metric listeners for use in calculating 
 * metrics for load balancing as well as failure detection. 
 * 
 * @author elandau
 *
 * @param <Host>
 */
public interface MetricsFactory<Host> extends Func2<Host, Action0, ClientMetricsListener> {
    /**
     * 
     * @param Host - Host being tracked
     * @param Acton0 - Action that will invoked when the host has been determined to have failed
     * @return ClientMetricsListener
     */
    @Override
    ClientMetricsListener call(Host host, Action0 failureAction);
    
    Class<?> getType();
}
