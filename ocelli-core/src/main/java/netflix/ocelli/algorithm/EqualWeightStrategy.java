package netflix.ocelli.algorithm;

import java.util.List;

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
public class EqualWeightStrategy<C> implements WeightingStrategy<C> {

    @Override
    public ClientsAndWeights<C> call(List<C> clients) {
        return new ClientsAndWeights<C>(clients, null);
    }
}
