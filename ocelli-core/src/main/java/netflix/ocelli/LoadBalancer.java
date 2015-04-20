package netflix.ocelli;

import java.util.List;

import rx.Observable;
import rx.functions.Action1;

/**
 * The LoadBalancer contract is similar to a Subject in that it receives (and caches) input
 * in the form of a List of active clients and emits a single client from that list based 
 * on the load balancing strategy for each subscription.
 * 
 * @author elandau
 *
 * @param <C>
 */
public abstract class LoadBalancer<C> extends Observable<C> implements Action1<List<C>> {
    protected LoadBalancer(OnSubscribe<C> f) {
        super(f);
    }
    
    
}
