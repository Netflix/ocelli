package netflix.ocelli.functions;

import rx.functions.Func1;
import netflix.ocelli.loadbalancer.weighting.EqualWeightStrategy;
import netflix.ocelli.loadbalancer.weighting.InverseMaxWeightingStrategy;
import netflix.ocelli.loadbalancer.weighting.LinearWeightingStrategy;
import netflix.ocelli.loadbalancer.weighting.WeightingStrategy;

public abstract class Weightings {
    /**
     * @return Strategy that provides a uniform weight to each client
     */
    public static <C> WeightingStrategy<C> uniform() {
        return new EqualWeightStrategy<C>();
    }
    
    /**
     * @param func
     * @return Strategy that uses the output of the function as the weight
     */
    public static <C> WeightingStrategy<C> identity(Func1<C, Integer> func) {
        return new LinearWeightingStrategy<C>(func);
    }

    /**
     * @param func
     * @return Strategy that sets the weight to the difference between the max
     *  value of all clients and the client value.
     */
    public static <C> WeightingStrategy<C> inverseMax(Func1<C, Integer> func) {
        return new InverseMaxWeightingStrategy<C>(func);
    }
}
