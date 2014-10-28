package rx.loadbalancer.loadbalancer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.functions.Func1;
import rx.loadbalancer.LoadBalancer;

/**
 * Very simple LoadBlancer that when queried gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class RoundRobinLoadBalancer<Client> implements LoadBalancer<Client> {
    private final Observable<ClientsAndWeights<Client>> source;
    
    private final AtomicInteger position = new AtomicInteger();
    
    public RoundRobinLoadBalancer(Observable<ClientsAndWeights<Client>> source) {
        this.source = source;
    }
    
    @Override
    public Observable<Client> select() {
        return source
            .concatMap(new Func1<ClientsAndWeights<Client>, Observable<Client>>() {
                int pos = position.incrementAndGet();

                @Override
                public Observable<Client> call(ClientsAndWeights<Client> hosts) {
                    List<Client> clients = hosts.getClients();
                    if (clients == null || hosts.isEmpty()) {
                        return Observable.empty();
                    }
                    return Observable.just(clients.get(pos++ % clients.size()));
                }
            });
    }
}
