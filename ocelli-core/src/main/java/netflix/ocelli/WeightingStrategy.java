package netflix.ocelli;

import java.util.List;

import netflix.ocelli.selectors.ClientsAndWeights;
import rx.functions.Func1;

/**
 * Contract for strategy to determine client weights from a list of clients
 * 
 * @author elandau
 *
 * @param <C>
 */
public interface WeightingStrategy<C> extends Func1<List<C>, ClientsAndWeights<C>> {
    /**
     * Run the weighting algorithm on the active set of clients and their associated statistics and 
     * return an object containing the weights
     */
    ClientsAndWeights<C> call(List<C> t1);
}
