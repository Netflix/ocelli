package netflix.ocelli;

import netflix.ocelli.loadbalancer.DefaultLoadBalancerBuilder;

public class Ocelli {
    public static <C> LoadBalancerBuilder<C> newDefaultLoadBalancerBuilder() {
        return new DefaultLoadBalancerBuilder<C>();
    }
}
