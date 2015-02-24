package netflix.ocelli.loadbalancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.loadbalancer.weighting.ClientsAndWeights;
import netflix.ocelli.loadbalancer.weighting.WeightingStrategy;
import rx.Subscriber;

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
public class RandomWeightedLoadBalancer<C> extends LoadBalancer<C> {
    public static <C> RandomWeightedLoadBalancer<C> create(final WeightingStrategy<C> strategy) {
        return new RandomWeightedLoadBalancer<C>(strategy);
    }

    private final AtomicReference<List<C>> clients;
    
    public RandomWeightedLoadBalancer(final WeightingStrategy<C> strategy) {
        this(strategy, new AtomicReference<List<C>>(new ArrayList<C>()));
    }

    RandomWeightedLoadBalancer(final WeightingStrategy<C> strategy, final AtomicReference<List<C>> clients) {
        super(new OnSubscribe<C>() {
            
            private final Random rand = new Random();
            
            @Override
            public void call(final Subscriber<? super C> s) {
                List<C> local = clients.get();

                final ClientsAndWeights<C> caw = strategy.call(local);
                if (!caw.isEmpty()) {
                    int total = caw.getTotalWeights();
                    if (total == 0) {
                        s.onNext(caw.getClient(rand.nextInt(caw.size())));
                        s.onCompleted();
                    }
                    else {
                        int pos = Collections.binarySearch(caw.getWeights(), rand.nextInt(total));
                        if (pos >= 0) {
                            pos = pos+1;
                        }
                        else {
                            pos = -(pos) - 1;
                        }

                        s.onNext(caw.getClient(pos));
                        s.onCompleted();
                    }
                }
                else {
                    s.onError(new NoSuchElementException("No servers available in the load balancer"));
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
