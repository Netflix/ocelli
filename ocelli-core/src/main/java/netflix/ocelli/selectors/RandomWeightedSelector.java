package netflix.ocelli.selectors;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.SelectionStrategy;
import netflix.ocelli.selectors.weighting.ClientsAndWeights;
import netflix.ocelli.selectors.weighting.WeightingStrategy;
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
public class RandomWeightedSelector<C> extends SelectionStrategy<C> {
    
    private final AtomicReference<C[]> clients;
    
    @SuppressWarnings("unchecked")
    public RandomWeightedSelector(final WeightingStrategy<C> strategy) {
        this(strategy, new Random(), new AtomicReference<C[]>((C[]) new Object[0]));
    }
    
    RandomWeightedSelector(final WeightingStrategy<C> strategy, final Random rand, final AtomicReference<C[]> clients) {
        super(new OnSubscribe<C>() {
                @Override
                public void call(Subscriber<? super C> s) {
                    final ClientsAndWeights<C> caw = strategy.call(clients.get());
                    if (!caw.isEmpty()) {
                        int total = caw.getTotalWeights();
                        if (total == 0) {
                            s.onNext(caw.getClient(rand.nextInt(caw.size())));
                        }
                        else {
                            int pos = Arrays.binarySearch(caw.getWeights(), rand.nextInt(total));
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
        this.clients = clients;

    }
    
    @Override
    public void setClients(C[] clients) {
        this.clients.set(clients);
    }
}
