package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.Subscriber;

/**
 * Very simple LoadBlancer that when subscribed to gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class RoundRobinLoadBalancer<C> extends BaseLoadBalancer<C> {
    public static <C> RoundRobinLoadBalancer<C> create(Observable<List<C>> source) {
        return new RoundRobinLoadBalancer<C>(source);
    }
    
    private final AtomicInteger position = new AtomicInteger(-1);
    
    RoundRobinLoadBalancer(final Observable<List<C>> source) {
        super(source);
    }

    @Override
    public void call(Subscriber<? super C> s) {
        List<C> local = clients.get();
        if (local.size() > 0) {
            int pos = position.incrementAndGet();
            if (pos < 0) {
                pos = -pos;
            }
            s.onNext(local.get(pos % local.size()));
            s.onCompleted();
        }                
        else {
            s.onError(new NoSuchElementException("No servers available in the load balancer"));
        }
    }
}
