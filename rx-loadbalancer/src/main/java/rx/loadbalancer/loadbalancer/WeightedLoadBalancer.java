package rx.loadbalancer.loadbalancer;

import rx.Observable;
import rx.functions.Func1;
import rx.loadbalancer.LoadBalancer;

/**
 * Load balancer that selects Clients based on weight
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class WeightedLoadBalancer<Client> implements LoadBalancer<Client> {
    
    private final Observable<ClientsAndWeights<Client>> source;
    
    private final WeightSelector func;
    
    public WeightedLoadBalancer(Observable<ClientsAndWeights<Client>> source, WeightSelector func) {
        this.source = source;
        this.func = func;
    }
    
    @Override
    public Observable<Client> select() {
        return source
            .concatMap(new Func1<ClientsAndWeights<Client>, Observable<Client>>() {
                int pos = -1;

                @Override
                public Observable<Client> call(ClientsAndWeights<Client> hosts) {
                    if (hosts.isEmpty()) {
                        return Observable.empty();
                    }
                    
                    if (pos == -1) {
                        pos = func.call(hosts.getWeights(), hosts.getTotalWeights());
                    }

                    return Observable.just(hosts.getClients().get(pos++ % hosts.getClients().size()));
                }
            });
    }
}
