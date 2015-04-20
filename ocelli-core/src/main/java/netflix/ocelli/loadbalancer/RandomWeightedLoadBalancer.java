package netflix.ocelli.loadbalancer;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import netflix.ocelli.loadbalancer.weighting.ClientsAndWeights;
import netflix.ocelli.loadbalancer.weighting.WeightingStrategy;

/**
 * Select the next element using a random number.  
 * 
 * The weights are sorted such as that each cell in the array represents the
 * sum of the previous weights plus its weight.  This structure makes it 
 * possible to do a simple binary search using a random number from 0 to 
 * total weights.
 * 
 * Runtime complexity is O(log N)
 * 
 * @author elandau
 *
 */
public class RandomWeightedLoadBalancer<C> extends SettableLoadBalancer<C> {
    public static <C> RandomWeightedLoadBalancer<C> create(final WeightingStrategy<C> strategy) {
        return new RandomWeightedLoadBalancer<C>(strategy);
    }

    private final WeightingStrategy<C> strategy;
    private final Random rand = new Random();

    public RandomWeightedLoadBalancer(final WeightingStrategy<C> strategy) {
        this.strategy = strategy;
    }

    @Override
    public C next() throws NoSuchElementException {
        List<C> local = clients.get();

        final ClientsAndWeights<C> caw = strategy.call(local);
        if (caw.isEmpty()) {
            throw new NoSuchElementException("No servers available in the load balancer");
        }
        
        int total = caw.getTotalWeights();
        if (total == 0) {
            return caw.getClient(rand.nextInt(caw.size()));
        }
        
        int pos = Collections.binarySearch(caw.getWeights(), rand.nextInt(total));
        return caw.getClient((pos >= 0) ? (pos+1) : (-(pos) - 1));
    }
}
