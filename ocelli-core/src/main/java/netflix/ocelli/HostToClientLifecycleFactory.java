package netflix.ocelli;

import rx.Notification;
import rx.Observable;
import rx.functions.Func1;

/**
 * Mapper from a Host to a client lifecycle observable
 * 
 * @author elandau
 *
 * @param <H>
 * @param <C>
 */
public interface HostToClientLifecycleFactory<H, C> extends Func1<H, Observable<Notification<C>>> {
}
