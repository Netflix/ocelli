package netflix.ocelli.algorithm;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.ManagedClient;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.metrics.RequestLatencyMetrics;
import netflix.ocelli.selectors.ClientsAndWeights;

public class LowestLatencyScoreStrategy<H, C> implements WeightingStrategy<H, C> {
    @Override
    public ClientsAndWeights<C> call(List<ManagedClient<H, C>> source) {
        List<C>  clients = new ArrayList<C>(source.size());
        List<Integer> weights = new ArrayList<Integer>();
        Integer min = 0;
        for (ManagedClient<H, C> context : source) {
            clients.add(context.getClient());
            
            int cur = (int) context.getMetrics(RequestLatencyMetrics.class).getLatencyScore();
            if (cur < min || min == 0) 
                min = cur;
            weights.add(cur);
        }

        int count = 0;
        for (int i = 0; i < weights.size(); i++) {
            int weight = weights.get(i);
            if (weight == 0) 
                count = 0;
            else 
                count = 100 * min / weight;
            weights.set(i, count);
        }
        return new ClientsAndWeights<C>(clients, null);
    }
}
