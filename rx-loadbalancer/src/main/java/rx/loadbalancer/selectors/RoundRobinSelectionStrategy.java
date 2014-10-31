package rx.loadbalancer.selectors;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.functions.Func1;

/**
 * Very simple LoadBlancer that when queried gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class RoundRobinSelectionStrategy<Client> implements Func1<ClientsAndWeights<Client>, Observable<Client>> {
    private final AtomicInteger position = new AtomicInteger();
    
    @Override
    public Observable<Client> call(ClientsAndWeights<Client> hosts) {
        List<Client> clients = hosts.getClients();
        if (clients == null || hosts.isEmpty()) {
            return Observable.empty();
        }
        return Observable.just(clients.get(position.incrementAndGet() % clients.size()));
    }
}
