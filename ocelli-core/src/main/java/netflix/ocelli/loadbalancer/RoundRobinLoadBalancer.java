package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;

/**
 * Very simple LoadBlancer that when subscribed to gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class RoundRobinLoadBalancer<T> extends AbstractLoadBalancer<T> {
    public static <T> RoundRobinLoadBalancer<T> create(Observable<List<T>> source) {
        return create(source, -1);
    }
    
    public static <T> RoundRobinLoadBalancer<T> create(Observable<List<T>> T, int seedPosition) {
        return new RoundRobinLoadBalancer<T>(T, seedPosition);
    }

    private final AtomicInteger position;

    public RoundRobinLoadBalancer(Observable<List<T>> source) {
        super(source);
        this.position = new AtomicInteger(new Random().nextInt(1000));
    }
    
    public RoundRobinLoadBalancer(Observable<List<T>> source, int seedPosition) {
        super(source);
        this.position = new AtomicInteger(seedPosition);
    }

    @Override
    protected T choose(List<T> local) throws NoSuchElementException {
        if (local.isEmpty()) {
            throw new NoSuchElementException("No servers available in the load balancer");
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
            return local.get(pos % local.size());
        }
    }
}
