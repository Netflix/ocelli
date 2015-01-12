package netflix.ocelli.loadbalancer;

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
    private final AtomicReference<C[]> clients;
    
    protected BaseLoadBalancer(final Observable<C[]> source, final AtomicReference<C[]> clients, rx.Observable.OnSubscribe<C> onSubscribe) {
        super(onSubscribe);
        
        this.s = source
            .subscribe(new Action1<C[]>() {
                @Override
                public void call(C[] t1) {
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
