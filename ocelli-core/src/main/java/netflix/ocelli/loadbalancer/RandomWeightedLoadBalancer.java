package netflix.ocelli.loadbalancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.loadbalancer.weighting.ClientsAndWeights;
import netflix.ocelli.loadbalancer.weighting.WeightingStrategy;
import rx.Observable;
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
public class RandomWeightedLoadBalancer<C> extends BaseLoadBalancer<C> {
    public static <C> RandomWeightedLoadBalancer<C> create(final Observable<List<C>> source, final WeightingStrategy<C> strategy) {
        return new RandomWeightedLoadBalancer<C>(source, strategy);
    }
    
    public RandomWeightedLoadBalancer(final Observable<List<C>> source, final WeightingStrategy<C> strategy) {
        this(source, strategy, new Random(), new AtomicReference<List<C>>(new ArrayList<C>()));
    }
    
    RandomWeightedLoadBalancer(final Observable<List<C>> source, final WeightingStrategy<C> strategy, final Random rand, final AtomicReference<List<C>> clients) {
        super(source, clients, new OnSubscribe<C>() {
                @Override
                public void call(Subscriber<? super C> s) {
                    final ClientsAndWeights<C> caw = strategy.call(clients.get());
                    if (!caw.isEmpty()) {
                        int total = caw.getTotalWeights();
                        if (total == 0) {
                            s.onNext(caw.getClient(rand.nextInt(caw.size())));
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
                        }
                        s.onCompleted();
                    }
                    else {
                        s.onError(new NoSuchElementException("No servers available in the load balancer"));
                    }
                }
            });
    }
}
