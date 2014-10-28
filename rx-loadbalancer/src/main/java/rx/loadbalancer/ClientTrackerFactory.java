package rx.loadbalancer;

import rx.functions.Action0;
import rx.functions.Func2;

/**
 * Factory for creating failure detectors that collect metrics and invoke a callback
 * when a failure occurs
 *
 * @param Host - Host being tracked
 * @param Acton0 - Active to invoke when the host fails
 * @return ClientTracker
 * 
 * @author elandau
 *
 * @param <Host>
 */
public interface ClientTrackerFactory<Host, ClientTracker> extends Func2<Host, Action0, ClientTracker> {

}
