package netflix.ocelli;

public interface PartitionedLoadBalancerBuilder<C, K> {
    PartitionedLoadBalancer<C, K> build();
}
