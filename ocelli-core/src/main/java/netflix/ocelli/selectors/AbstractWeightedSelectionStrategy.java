package netflix.ocelli.selectors;

import java.util.NoSuchElementException;

import netflix.ocelli.SelectionStrategy;
import netflix.ocelli.weighted.WeightingStrategy;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

/**
 * Load balancer that selects Clients based on weight
 * 
 * @author elandau
 *
 * @param <C>
 */
public abstract class AbstractWeightedSelectionStrategy<C> implements SelectionStrategy<C> {
    
    private final WeightingStrategy<C> strategy;
    private volatile C[] clients;
    
    public AbstractWeightedSelectionStrategy(WeightingStrategy<C> strategy) {
        this.strategy = strategy;
    }
    
    @Override
    public Observable<C> call() {
        return Observable.create(new OnSubscribe<C>() {
            @Override
            public void call(Subscriber<? super C> s) {
                final ClientsAndWeights<C> caw = strategy.call(clients);
                if (!caw.isEmpty()) {
                    int index = nextIndex(caw);
                    s.onNext(caw.getClient(index));
                }
                else {
                    s.onError(new NoSuchElementException("No servers available in the load balancer"));
                }
                s.onCompleted();
            }
        });
    }

    @Override
    public void setClients(C[] clients) {
        this.clients = clients;
    }
    
    protected abstract int nextIndex(ClientsAndWeights<C> caw);
}
