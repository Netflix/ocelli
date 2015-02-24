package netflix.ocelli.loadbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.LoadBalancer;
import rx.Subscriber;

/**
 * Very simple LoadBlancer that when subscribed to gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class RoundRobinLoadBalancer<C> extends LoadBalancer<C> {
    public static <C> RoundRobinLoadBalancer<C> create() {
        return create(-1);
    }
    
    public static <C> RoundRobinLoadBalancer<C> create(int seedPosition) {
        return new RoundRobinLoadBalancer<C>(seedPosition);
    }

    private final AtomicReference<List<C>> clients;
    
    RoundRobinLoadBalancer(int seedPosition) {
        this(new AtomicInteger(seedPosition), new AtomicReference<List<C>>(new ArrayList<C>()));
    }

    private RoundRobinLoadBalancer(final AtomicInteger position, final AtomicReference<List<C>> clients) {
        super(new OnSubscribe<C>() {
            @Override
            public void call(final Subscriber<? super C> s) {
                List<C> local = clients.get();
                if (local.isEmpty()) {
                    s.onError(new NoSuchElementException("No servers available in the load balancer"));
                }
                else {
                    int pos = position.incrementAndGet();
                    while (pos < 0) {
                        if (position.compareAndSet(pos, 0)) {
                            pos = 0;
                            break;
                        }
                        pos = position.incrementAndGet();
                    }
                    s.onNext(local.get(pos % local.size()));
                    s.onCompleted();
                }
            }
        });
        
        this.clients = clients;
    }
    
    @Override
    public void call(List<C> t) {
        this.clients.set(t);
    }

}
