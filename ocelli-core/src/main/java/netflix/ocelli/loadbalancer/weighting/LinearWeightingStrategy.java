package netflix.ocelli.loadbalancer.weighting;

import java.util.ArrayList;
import java.util.List;

import rx.functions.Func1;

public class LinearWeightingStrategy<C> implements WeightingStrategy<C> {
    
    private Func1<C, Integer> func;

    public LinearWeightingStrategy(Func1<C, Integer> func) {
        this.func = func;
    }
    
    @Override
    public ClientsAndWeights<C> call(List<C> clients) {
        ArrayList<Integer> weights = new ArrayList<Integer>(clients.size());
        
        if (clients.size() > 0) {
            for (int i = 0; i < clients.size(); i++) {
                weights.add(func.call(clients.get(i)));
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
