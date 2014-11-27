package netflix.ocelli;

import rx.Observable;

/**
 * Strategy for selecting a single server via an Observable<C> from an immutable
 * set of servers.
 * 
 * @author elandau
 *
 * @param <C>
 */
public abstract class SelectionStrategy<C> extends Observable<C> {
    protected SelectionStrategy(rx.Observable.OnSubscribe<C> f) {
        super(f);
    }

    /**
     * Set the list of clients to use for selection.  This method is called whenever the client
     * list changes.
     * 
     * @param clients
     */
    public abstract void setClients(C[] clients);
}
