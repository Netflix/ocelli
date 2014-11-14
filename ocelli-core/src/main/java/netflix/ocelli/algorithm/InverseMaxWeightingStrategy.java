package netflix.ocelli.algorithm;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;
import rx.functions.Func1;

/**
 * Weighting strategy that gives an inverse weight to the highest rate.  Using
 * this strategy higher input values receive smaller weights. 
 * 
 * For example, if the weight is based on pending requests then an input of
 * [1, 5, 10, 1] pending request counts would yield the following weights
 * [10, 6, 1, 10] using the formula : w(i) = max - w(i) + 1
 * 
 * Note that 1 is added to ensure that we don't have a 0 weight, which is invalid.
 * 
 * @author elandau
 *
 * @param <C>
 */
public class InverseMaxWeightingStrategy<C> implements WeightingStrategy<C> {
    
    private Func1<C, Integer> func;

    public InverseMaxWeightingStrategy(Func1<C, Integer> func) {
        this.func = func;
    }
    
    @Override
    public ClientsAndWeights<C> call(List<C> source) {
        List<C>  clients = new ArrayList<C>(source.size());
        List<Integer> weights = new ArrayList<Integer>(source.size());
        
        if (source.size() > 0) {
            Integer max = 0;
            for (C context : source) {
                clients.add(context);
                int weight = func.call(context);
                if (weight > max) {
                    max = weight;
                }
                weights.add(weight);
            }
    
            int sum = 0;
            for (int i = 0; i < weights.size(); i++) {
                sum += (max - weights.get(i)) + 1;
                weights.set(i, sum);
            }
        }
        return new ClientsAndWeights<C>(clients, weights);
    }
}
