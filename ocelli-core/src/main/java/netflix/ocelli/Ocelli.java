package netflix.ocelli;

import netflix.ocelli.loadbalancer.LoadBalancerBuilder;
import rx.Observable;

/**
 * An entry point into creating instances of {@link LoadBalancer} with various flavors.
 */
public final class Ocelli {

    private Ocelli() {
    }

    /**
     * Creates an instance of {@link LoadBalancer} that will load balance on the members provided by the
     * {@code membershipSource}
     *
     * @param membershipSource Source of all members managed by the returned {@link LoadBalancer}
     *
     * @param <M> Generic type of the members.
     *
     * @return An instance of {@link LoadBalancer} load balancing on the members provided by the {@code membershipSource}
     */
    public static <M> LoadBalancer<M> forMembershipSource(Observable<MembershipEvent<M>> membershipSource,
                                                          MetricsFactory<M, M> metricsMapper) {
        return newBuilder(membershipSource, metricsMapper).build();
    }

    /**
     * Creates an instance of {@link LoadBalancerBuilder} that will load balance on the members provided by the
     * {@code membershipSource}
     *
     * @param membershipSource Source of all members managed by the returned {@link LoadBalancer}
     *
     * @return An instance of {@link LoadBalancer} load balancing on the members provided by the {@code membershipSource}
     */
    public static <M> LoadBalancerBuilder<M, M> newBuilder(Observable<MembershipEvent<M>> membershipSource,
                                                           MetricsFactory<M, M> metricsMapper) {
        return new LoadBalancerBuilder<>(membershipSource, metricsMapper);
    }
}
