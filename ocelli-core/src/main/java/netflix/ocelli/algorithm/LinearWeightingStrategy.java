package netflix.ocelli.algorithm;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;
import rx.functions.Func1;

public class LinearWeightingStrategy<C> implements WeightingStrategy<C> {
    
    private Func1<C, Integer> func;

    public LinearWeightingStrategy(Func1<C, Integer> func) {
        this.func = func;
    }
    
    @Override
    public ClientsAndWeights<C> call(List<C> source) {
        List<C>  clients = new ArrayList<C>(source.size());
        List<Integer> weights = new ArrayList<Integer>(source.size());
        
        if (source.size() > 0) {
            for (C context : source) {
                clients.add(context);
                weights.add(func.call(context));
            }
    
            int sum = 0;
            for (int i = 0; i < weights.size(); i++) {
                sum += weights.get(i);
                weights.set(i, sum);
            }
        }
        return new ClientsAndWeights<C>(clients, weights);
    }
}
