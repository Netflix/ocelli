package netflix.ocelli;

import netflix.ocelli.loadbalancer.DefaultLoadBalancerBuilder;
import rx.Observable;

public class LoadBalancers {

    public static <C> LoadBalancerBuilder<C> newBuilder(Observable<MembershipEvent<C>> hostSource) {
        return new DefaultLoadBalancerBuilder<C>(hostSource);
    }

    public static <C> LoadBalancer<C> fromHostSource(Observable<MembershipEvent<C>> hostSource) {
        return newBuilder(hostSource).build();
    }
}
