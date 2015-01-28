package netflix.ocelli.loadbalancer.weighting;

import java.util.List;


/**
 * Strategy where all clients have the same weight
 * @author elandau
 *
 * @param <Host>
 * @param <C>
 * @param <Metrics>
 */
public class EqualWeightStrategy<C> implements WeightingStrategy<C> {

    @Override
    public ClientsAndWeights<C> call(List<C> clients) {
        return new ClientsAndWeights<C>(clients, null);
    }
}
