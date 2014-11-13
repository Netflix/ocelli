package netflix.ocelli;

import netflix.ocelli.selectors.ClientsAndWeights;
import rx.Observable;
import rx.functions.Func1;

public interface LoadBalancerBuilder<C> {
    /**
     * Arbitrary name used in debugging
     * @param name
     */
    LoadBalancerBuilder<C> withName(String name);
    
    /**
     * Strategy used to determine the delay time in msec based on the quarantine 
     * count.  The count is incremented by one for each failure detections and reset
     * once the host is back to normal.
     */
    LoadBalancerBuilder<C> withQuarantineStrategy(Func1<Integer, Long> quaratineDelayStrategy);
    
    /**
     * Strategy used to determine how many hosts should be active.
     * This strategy is invoked whenever a host is added or removed from the pool
     */
    LoadBalancerBuilder<C> withActiveClientCountStrategy(Func1<Integer, Integer> activeClientCountStrategy);
    
    /**
     * Source for host membership events
     */
    LoadBalancerBuilder<C> withMembershipSource(Observable<MembershipEvent<C>> hostSource);
    
    /**
     * Strategy use to calculate weights for active clients
     */
    LoadBalancerBuilder<C> withWeightingStrategy(WeightingStrategy<C> algorithm);
    
    /**
     * Strategy used to select hosts from the calculated weights.  
     * @param selectionStrategy
     */
    LoadBalancerBuilder<C> withSelectionStrategy(Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy);
    
    /**
     * The failure detector returns an Observable that will emit a Throwable for each 
     * failure of the client.  The load balancer will quaratine the client in response.
     * @param failureDetector
     */
    LoadBalancerBuilder<C> withFailureDetector(FailureDetectorFactory<C> failureDetector);
    
    /**
     * The connector can be used to prime a client prior to activating it in the connection
     * pool.  
     * @param clientConnector
     */
    LoadBalancerBuilder<C> withClientConnector(ClientConnector<C> clientConnector);
    
    /**
     * Partition the load balancer using the provided function
     * 
     * @param partitioner
     * @return
     */
    <K> PartitionedLoadBalancerBuilder<C, K> withPartitioner(Func1<C, Observable<K>> partitioner);
    
    LoadBalancer<C> build();
}
