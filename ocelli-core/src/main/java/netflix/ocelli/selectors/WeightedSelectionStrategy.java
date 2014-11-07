package netflix.ocelli.selectors;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func1;

/**
 * Load balancer that selects Clients based on weight
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class WeightedSelectionStrategy<Client> implements Func1<ClientsAndWeights<Client>, Observable<Client>> {
    
    private final WeightSelector func;
    
    public WeightedSelectionStrategy(WeightSelector func) {
        this.func = func;
    }
    
    @Override
    public Observable<Client> call(final ClientsAndWeights<Client> hosts) {
        if (hosts.isEmpty()) {
            return Observable.empty();
        }
        
        return Observable.create(new OnSubscribe<Client>() {
            int pos = func.call(hosts.getWeights(), hosts.getTotalWeights());
            
            @Override
            public void call(Subscriber<? super Client> t1) {
                t1.onNext(hosts.getClients().get(pos++ % hosts.getClients().size()));
                t1.onCompleted();
            }
        });
    }
}
