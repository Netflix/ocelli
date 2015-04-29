package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import netflix.ocelli.LoadBalancerStrategy;
import rx.functions.Func2;

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
    public static <T> ChoiceOfTwoLoadBalancer<T> create(final Func2<T, T, T> func) {
        return new ChoiceOfTwoLoadBalancer<T>(func);
    }

    private final Func2<T, T, T> func;
    private final Random rand = new Random();
    
    public ChoiceOfTwoLoadBalancer(final Func2<T, T, T> func) {
        this.func = func;
    }

    @Override
    public T choose(List<T> local) throws NoSuchElementException {
        if (local.isEmpty()) {
            throw new NoSuchElementException("No servers available in the load balancer");
        }
        else if (local.size() == 1) {
            return local.get(0);
        }
        else {
            int first  = rand.nextInt(local.size());
            int second = (rand.nextInt(local.size()-1) + first + 1) % local.size();
            
            return func.call(local.get(first), local.get(second));
        }
    }
}
