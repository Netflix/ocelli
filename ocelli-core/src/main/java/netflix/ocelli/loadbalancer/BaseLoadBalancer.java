package netflix.ocelli.loadbalancer;

import java.util.ArrayList;
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
    protected final AtomicReference<List<C>> clients = new AtomicReference<List<C>>(new ArrayList<C>());
    
    protected BaseLoadBalancer(final Observable<List<C>> source) {
        this.s = source
            .subscribe(new Action1<List<C>>() {
                @Override
                public void call(List<C> t1) {
                    clients.set(t1);
                }
            });
    }
    
    public void shutdown() {
        s.unsubscribe();
    }

    public Observable<C> all() {
        return Observable.from(clients.get());
    }
}
