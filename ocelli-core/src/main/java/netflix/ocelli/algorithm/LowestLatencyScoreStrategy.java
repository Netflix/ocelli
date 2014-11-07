package netflix.ocelli.algorithm;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.ManagedClient;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.metrics.ClientMetrics;
import netflix.ocelli.selectors.ClientsAndWeights;

public class LowestLatencyScoreStrategy<Host, Client, Tracker extends ClientMetrics> implements WeightingStrategy<Host, Client, Tracker> {
    @Override
    public ClientsAndWeights<Client> call(List<ManagedClient<Host, Client, Tracker>> source) {
        List<Client>  clients = new ArrayList<Client>(source.size());
        List<Integer> weights = new ArrayList<Integer>();
        Integer min = 0;
        for (ManagedClient<Host, Client, Tracker> context : source) {
            clients.add(context.getClient());
            
            int cur = (int) context.getMetrics().getLatencyScore();
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
        return new ClientsAndWeights<Client>(clients, null);
    }
}
