package rx.loadbalancer.algorithm;

import java.util.ArrayList;
import java.util.List;

import rx.loadbalancer.ManagedClient;
import rx.loadbalancer.WeightingStrategy;
import rx.loadbalancer.metrics.ClientMetrics;
import rx.loadbalancer.selectors.ClientsAndWeights;

public class LeastLoadedStrategy<Host, Client, Tracker extends ClientMetrics> implements WeightingStrategy<Host, Client, Tracker> {
    @Override
    public ClientsAndWeights<Client> call(List<ManagedClient<Host, Client, Tracker>> source) {
        List<Client>  clients = new ArrayList<Client>(source.size());
        List<Integer> weights = new ArrayList<Integer>();
        Integer max = 0;
        for (ManagedClient<Host, Client, Tracker> context : source) {
            clients.add(context.getClient());
            
            int cur = (int) context.getMetrics().getPendingRequestCount();
            if (cur > max) 
                max = cur;
            weights.add(cur);
        }

        int count = 0;
        for (int i = 0; i < weights.size(); i++) {
            count += max - weights.get(i) + 1;
            weights.set(i, count);
        }
        System.out.println(weights);
        return new ClientsAndWeights<Client>(clients, null);
    }
}
