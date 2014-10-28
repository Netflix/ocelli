package rx.loadbalancer.loadbalancer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Select the next element using round robin so that all server of a certain
 * weight are selected first before moving on to the next server.  
 * 
 * The weights are sorted such as that each cell in the array represents the
 * sum of the previous weight plus it's weight.
 * 
 * Worst case runtime complexity is O(log N)
 * 
 * @author elandau
 *
 */
public class RoundRobinWeightSelector implements WeightSelector {

    private AtomicInteger position = new AtomicInteger();
    
    @Override
    public Integer call(List<Integer> weights, Integer count) {
        return Collections.binarySearch(weights, position.incrementAndGet() % count);
    }
}
