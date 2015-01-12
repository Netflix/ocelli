package netflix.ocelli.loadbalancer;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.ClientCollector;
import netflix.ocelli.MembershipEvent;
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
    public static <C> RoundRobinLoadBalancer<C> create(Observable<C[]> source) {
        return new RoundRobinLoadBalancer<C>(source);
    }
    
    public static <C> RoundRobinLoadBalancer<C> from(Observable<MembershipEvent<C>> source) {
        return new RoundRobinLoadBalancer<C>(source.map(new ClientCollector<C>()));
    }
    
    @SuppressWarnings("unchecked")
    public RoundRobinLoadBalancer(Observable<C[]> source) {
        this(source, new AtomicReference<C[]>((C[]) new Object[0]), new AtomicInteger(-1));
    }
    
    RoundRobinLoadBalancer(final Observable<C[]> source, final AtomicReference<C[]> clients, final AtomicInteger position) {
        super(source, clients, new OnSubscribe<C>() {
            @Override
            public void call(Subscriber<? super C> s) {
                C[] internal = clients.get();
                if (internal.length > 0) {
                    int pos = position.incrementAndGet();
                    if (pos < 0) {
                        pos = -pos;
                    }
                    s.onNext(internal[pos % internal.length]);
                    s.onCompleted();
                }                
                else {
                    s.onError(new NoSuchElementException("No servers available in the load balancer"));
                }
            }
        });
    }
}
