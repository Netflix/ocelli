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
        List<Integer> weights = new ArrayList<Integer>();
        
        Integer max = 0;
        for (ClientAndMetrics<C, M> context : source) {
            clients.add(context.getClient());
            
            int cur = func.call(context.getMetrics());
            if (cur > max) 
                max = cur;
            weights.add(cur);
        }

        int count = 0;
        for (int i = 0; i < weights.size(); i++) {
            count += max - weights.get(i) + 1;
            weights.set(i, count);
        }
        return new ClientsAndWeights<C>(clients, weights);
    }
}
