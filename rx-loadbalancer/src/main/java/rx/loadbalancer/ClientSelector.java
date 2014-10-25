package rx.loadbalancer;

import java.util.List;

import rx.Observable;

/**
 * A concrete ClientSelector keeps track of all available hosts and returns 
 * the most recent immutable collection of connected {@link Client}'s.
 * 
 * @author elandau
 */
public interface ClientSelector<Client> {
    /**
     * @return Observable that emits a single List<Client> for all connected hosts
     */
    Observable<List<Client>> aquire();
    
    /**
     * Prime Clients using the maximum number of allows Clients
     * 
     * @return Observable that will emit the primed clients 
     */
//    Observable<Client> prime();
    
    /**
     * Prime N clients
     * @param count
     * @return
     */
//    Observable<Client> prime(int count);
}
