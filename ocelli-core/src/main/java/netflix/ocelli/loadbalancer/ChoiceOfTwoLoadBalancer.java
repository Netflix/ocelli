package netflix.ocelli.loadbalancer;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import netflix.ocelli.LoadBalancerStrategy;

/**
 * This selector chooses 2 random hosts and picks the host with the 'best' 
 * performance where that determination is deferred to a customizable function.
 * 
 * This implementation is based on the paper 'The Power of Two Choices in 
 * Randomized Load Balancing' http://www.eecs.harvard.edu/~michaelm/postscripts/tpds2001.pdf
 * This paper states that selecting the best of 2 random servers results in an 
 * exponential improvement over selecting a single random node (also includes
 * round robin) but that adding a third (or more) servers does not yield a significant
 * performance improvement.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class ChoiceOfTwoLoadBalancer<T> implements LoadBalancerStrategy<T> {
    public static <T> ChoiceOfTwoLoadBalancer<T> create(final Comparator<T> func) {
        return new ChoiceOfTwoLoadBalancer<T>(func);
    }

    private final Comparator<T> func;
    private final Random rand = new Random();
    
    public ChoiceOfTwoLoadBalancer(final Comparator<T> func2) {
        this.func = func2;
    }

    @Override
    public T choose(List<T> candidates) throws NoSuchElementException {
        if (candidates.isEmpty()) {
            throw new NoSuchElementException("No servers available in the load balancer");
        }
        else if (candidates.size() == 1) {
            return candidates.get(0);
        }
        else {
            int pos  = rand.nextInt(candidates.size());
            T first  = candidates.get(pos);
            T second = candidates.get((rand.nextInt(candidates.size()-1) + pos + 1) % candidates.size());
            
            return func.compare(first, second) >= 0 ? first : second;
        }
    }
}
