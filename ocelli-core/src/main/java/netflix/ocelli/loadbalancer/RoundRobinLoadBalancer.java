package netflix.ocelli.loadbalancer;

import netflix.ocelli.LoadBalancerStrategy;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Very simple LoadBlancer that when subscribed to gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 */
public class RoundRobinLoadBalancer<T> implements LoadBalancerStrategy<T> {
    public static <T> RoundRobinLoadBalancer<T> create() {
        return create(new Random().nextInt(1000));
    }
    
    public static <T> RoundRobinLoadBalancer<T> create(int seedPosition) {
        return new RoundRobinLoadBalancer<T>(seedPosition);
    }

    private final AtomicInteger position;

    public RoundRobinLoadBalancer() {
        position = new AtomicInteger(new Random().nextInt(1000));
    }
    
    public RoundRobinLoadBalancer(int seedPosition) {
        position = new AtomicInteger(seedPosition);
    }

    /**
     * @throws NoSuchElementException
     */
    @Override
    public T choose(List<T> local) {
        if (local.isEmpty()) {
            throw new NoSuchElementException("No servers available in the load balancer");
        }
        else {
            int pos = Math.abs(position.incrementAndGet());
            return local.get(pos % local.size());
        }
    }
}
