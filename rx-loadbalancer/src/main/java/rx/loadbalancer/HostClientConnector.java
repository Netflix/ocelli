package rx.loadbalancer;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func3;

/**
 * 
 * @author elandau
 */
public interface HostClientConnector<Host, Client> extends Func3<Host, Action1<ClientEvent>, Observable<Void>, Observable<Client>>{
    /**
     * Called to open a client connection
     * 
     * @param host      Host address
     * @param events    Action1 to be called for client events sources at the Client
     * @param signal    Observable of a shutdown event
     */
    public Observable<Client> call(Host host, Action1<ClientEvent> events, Observable<Void> signal);
}
