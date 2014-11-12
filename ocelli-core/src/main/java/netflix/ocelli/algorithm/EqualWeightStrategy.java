package netflix.ocelli.algorithm;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.ClientAndMetrics;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;

/**
 * Strategy where all clients have the same weight
 * @author elandau
 *
 * @param <Host>
 * @param <C>
 * @param <Metrics>
 */
public class EqualWeightStrategy<C, M> implements WeightingStrategy<C, M> {

    @Override
    public ClientsAndWeights<C> call(List<ClientAndMetrics<C,M>> t1) {
        List<C> clients = new ArrayList<C>(t1.size());
        for (ClientAndMetrics<C,M> context : t1) {
            clients.add(context.getClient());
        }
        return new ClientsAndWeights<C>(clients, null);
    }
}
