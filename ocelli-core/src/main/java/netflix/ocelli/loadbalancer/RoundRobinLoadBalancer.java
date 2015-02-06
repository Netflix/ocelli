package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.LoadBalancer;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

/**
 * Very simple LoadBlancer that when subscribed to gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class RoundRobinLoadBalancer<C> extends BaseLoadBalancer<C> {
    public static <C> RoundRobinLoadBalancer<C> from(Observable<List<C>> source) {
        return from(source, -1);
    }
    
    public static <C> RoundRobinLoadBalancer<C> from(Observable<List<C>> source, int seedPosition) {
        return new RoundRobinLoadBalancer<C>(source, seedPosition);
    }
    
    public static <C> Func1<Observable<List<C>>, LoadBalancer<C>> factory() {
        return new Func1<Observable<List<C>>, LoadBalancer<C>>() {
            @Override
            public LoadBalancer<C> call(Observable<List<C>> t1) {
                return from(t1, new Random().nextInt());
            }
        };
    };
    
    private final AtomicInteger position;
    
    RoundRobinLoadBalancer(final Observable<List<C>> source, int seedPosition) {
        super(source);
        position = new AtomicInteger(-1);
    }

    @Override
    public void call(Subscriber<? super C> s) {
        List<C> local = clients.get();
        if (local.size() > 0) {
            int pos = position.incrementAndGet();
            while (pos < 0) {
                position.compareAndSet(pos, 0);
                pos = position.incrementAndGet();
            }
            s.onNext(local.get(pos % local.size()));
            s.onCompleted();
        }                
        else {
            s.onError(new NoSuchElementException("No servers available in the load balancer"));
        }
    }
}
