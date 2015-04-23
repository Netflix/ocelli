package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.NoSuchElementException;

import netflix.ocelli.LoadBalancer;

/**
 * Composite load balancer that cascades through a set of load balancers until one has a client.
 * This type load balancer should be used for use cases such as DC and Vip failover.
 * 
 * @author elandau
 *
 * @param <C>
 */
public class FallbackLoadBalancer<C> extends LoadBalancer<C> {

    private final List<LoadBalancer<C>> lbs;

    public FallbackLoadBalancer(final List<LoadBalancer<C>> lbs) {
        this.lbs = lbs;
    }

    @Override
    public C next() throws NoSuchElementException {
        for (LoadBalancer<C> lb : lbs) {
            try {
                return lb.next();
            }
            catch (NoSuchElementException e) {
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public void shutdown() {
    }
}
