package netflix.ocelli.selectors;

import java.util.Arrays;
import java.util.Random;

import netflix.ocelli.weighted.WeightingStrategy;

/**
 * Select the next element using a random number.  
 * 
 * The weights are sorted such as that each cell in the array represents the
 * sum of the previous weight plus it's weight.
 * 
 * Worst case runtime complexity is O(log N)
 * 
 * @author elandau
 *
 */
public class RandomWeightedSelector<C> extends AbstractWeightedSelectionStrategy<C> {

    public RandomWeightedSelector(WeightingStrategy<C> strategy) {
        super(strategy);
    }

    private Random random = new Random();
    
    @Override
    protected int nextIndex(ClientsAndWeights<C> caw) {
        int total = caw.getTotalWeights();
        if (total == 0) {
            return random.nextInt(caw.size());
        }
                         
        int next = random.nextInt(total);
        int pos = Arrays.binarySearch(caw.getWeights(), next);
        if (pos >= 0) {
            return pos+1;
        }
        else {
            return -(pos) - 1;
        }
    }
}
