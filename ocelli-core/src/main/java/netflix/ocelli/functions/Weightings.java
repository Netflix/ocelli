package netflix.ocelli.functions;

import rx.functions.Func1;
import netflix.ocelli.selectors.weighting.EqualWeightStrategy;
import netflix.ocelli.selectors.weighting.InverseMaxWeightingStrategy;
import netflix.ocelli.selectors.weighting.LinearWeightingStrategy;
import netflix.ocelli.selectors.weighting.WeightingStrategy;

public class Weightings {
    /**
     * @return Strategy that provides a uniform weight to each client
     */
    public <C> WeightingStrategy<C> uniform() {
        return new EqualWeightStrategy<C>();
    }
    
    /**
     * @param func
     * @return Strategy that uses the output of the function as the weight
     */
    public <C> WeightingStrategy<C> identity(Func1<C, Integer> func) {
        return new LinearWeightingStrategy<C>(func);
    }

    /**
     * @param func
     * @return Strategy that sets the weight to the difference between the max
     *  value of all clients and the client value.
     */
    public <C> WeightingStrategy<C> inverseMax(Func1<C, Integer> func) {
        return new InverseMaxWeightingStrategy<C>(func);
    }
}
