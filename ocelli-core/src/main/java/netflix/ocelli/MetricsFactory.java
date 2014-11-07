package netflix.ocelli;

import rx.functions.Action0;
import rx.functions.Func2;

/**
 * Factory for creating failure detectors that collect metrics and invoke a callback
 * when a failure occurs
 * 
 * @author elandau
 *
 * @param <Host>
 */
public interface MetricsFactory<Host, Metrics> extends Func2<Host, Action0, Metrics> {
    /**
     * 
     * @param Host - Host being tracked
     * @param Acton0 - Action that will invoked when the host fails
     * @return Metrics
     */
    @Override
    Metrics call(Host host, Action0 failureAction);
}
