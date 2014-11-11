package netflix.ocelli;

import netflix.ocelli.metrics.ClientMetricsListener;
import rx.Observable;
import rx.functions.Func3;

/**
 * Contract for creating a Client implementation from a Host address.  
 * The load balancer will invoke this connector for each host that has been determined
 * to be available to access traffic.  
 * 
 * @author elandau
 */
public interface HostClientConnector<Host, Client> extends Func3<Host, ClientMetricsListener, Observable<Void>, Observable<Client>>{
    /**
     * Called to create a client implementation.  The implementation may include priming connections in 
     * which case the Client should not be emitted until at least one connection has been established.
     * 
     * @param host      Host address
     * @param events    Interface to be called for client events originating at the Client
     * @param signal    Observable of a shutdown event
     */
    public Observable<Client> call(Host host, ClientMetricsListener events, Observable<Void> signal);
}
