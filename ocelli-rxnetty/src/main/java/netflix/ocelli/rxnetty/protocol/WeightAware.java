package netflix.ocelli.rxnetty.protocol;

import io.reactivex.netty.client.ConnectionProvider;
import netflix.ocelli.loadbalancer.RandomWeightedLoadBalancer;

/**
 * A property for RxNetty listeners that are used to also define weights for a particular host, typically in a
 * {@link RandomWeightedLoadBalancer}
 */
public interface WeightAware {

    /**
     * Returns the current weight of the associated host with this object.
     * <b>This method will be called every time {@link ConnectionProvider#nextConnection()} is called for every active
     * hosts, so it is recommended to not do any costly processing in this method, it should typically be a lookup of
     * an already calculated value.</b>
     *
     * @return The current weight.
     */
    int getWeight();

}
