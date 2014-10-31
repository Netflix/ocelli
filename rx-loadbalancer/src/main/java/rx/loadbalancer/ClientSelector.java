package rx.loadbalancer;

import rx.Observable;
import rx.loadbalancer.loadbalancer.ClientsAndWeights;

/**
 * A concrete ClientSelector keeps track of all available hosts and returns 
 * the most recent immutable collection of connected {@link Client}'s.
 * 
 * @author elandau
 */
public interface ClientSelector<Host, Client> {
    /**
     * @return Observable that emits a single List<Client> for all connected hosts
     */
    Observable<ClientsAndWeights<Client>> acquire();

    /**
     * @return Stream of all host events
     */
    Observable<HostEvent<Host>> events();
}
