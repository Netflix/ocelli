package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.LoadBalancer;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;

/**
 * Base for all LoadBalancers will emit a single C based on the load balancing
 * strategy when subscribed to.
 * 
 * @author elandau
 *
 * @param <C>
 */
public abstract class BaseLoadBalancer<C> extends LoadBalancer<C> {
    private final Subscription s;
    private final AtomicReference<List<C>> clients;
    
    protected BaseLoadBalancer(final Observable<List<C>> source, final AtomicReference<List<C>> clients, rx.Observable.OnSubscribe<C> onSubscribe) {
        super(onSubscribe);
        
        this.s = source
            .subscribe(new Action1<List<C>>() {
                @Override
                public void call(List<C> t1) {
                    clients.set(t1);
                }
            });
        
        this.clients = clients;
    }
    
    public void shutdown() {
        s.unsubscribe();
    }

    public Observable<C> all() {
        return Observable.from(clients.get());
    }
}
