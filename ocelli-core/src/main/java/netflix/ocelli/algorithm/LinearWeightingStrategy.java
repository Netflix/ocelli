package netflix.ocelli.algorithm;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.ClientAndMetrics;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;
import rx.functions.Func1;

public class LinearWeightingStrategy<C, M> implements WeightingStrategy<C, M> {
    
    private Func1<M, Integer> func;

    public LinearWeightingStrategy(Func1<M, Integer> func) {
        this.func = func;
    }
    
    @Override
    public ClientsAndWeights<C> call(List<ClientAndMetrics<C,M>> source) {
        List<C>  clients = new ArrayList<C>(source.size());
        List<Integer> weights = new ArrayList<Integer>(source.size());
        
        if (source.size() > 0) {
            for (ClientAndMetrics<C, M> context : source) {
                clients.add(context.getClient());
                weights.add(func.call(context.getMetrics()));
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
