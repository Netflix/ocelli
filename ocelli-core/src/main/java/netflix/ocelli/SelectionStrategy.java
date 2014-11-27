package netflix.ocelli;

import rx.Observable;
import rx.functions.Func0;

/**
 * Strategy for selecting a single server via an Observable<C> from an immutable
 * set of servers.
 * 
 * @author elandau
 *
 * @param <C>
 */
public interface SelectionStrategy<C> extends Func0<Observable<C>> {
    /**
     * Set the list of clients to use for selection.  This method is called whenever the client
     * list changes.
     * 
     * @param clients
     */
    public void setClients(C[] clients);
}
