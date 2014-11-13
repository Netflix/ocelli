package netflix.ocelli.loadbalancer;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.ClientConnector;
import netflix.ocelli.FailureDetectorFactory;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.LoadBalancerBuilder;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.PartitionedLoadBalancer;
import netflix.ocelli.PartitionedLoadBalancerBuilder;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.algorithm.EqualWeightStrategy;
import netflix.ocelli.functions.Connectors;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Failures;
import netflix.ocelli.functions.Functions;
import netflix.ocelli.selectors.ClientsAndWeights;
import netflix.ocelli.selectors.RoundRobinSelectionStrategy;
import rx.Observable;
import rx.functions.Func1;

public class DefaultLoadBalancerBuilder<C> implements LoadBalancerBuilder<C> {
    private Observable<MembershipEvent<C>>   hostSource;
    private String                      name = "<unnamed>";
    private WeightingStrategy<C>        weightingStrategy = new EqualWeightStrategy<C>();
    private Func1<Integer, Integer>     connectedHostCountStrategy = Functions.identity();
    private Func1<Integer, Long>        quaratineDelayStrategy = Delays.fixed(10, TimeUnit.SECONDS);
    private Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy = new RoundRobinSelectionStrategy<C>();
    private FailureDetectorFactory<C>   failureDetector = Failures.never();
    private ClientConnector<C>          clientConnector = Connectors.immediate();
    
    /**
     * Arbitrary name assigned to the connection pool, mostly for debugging purposes
     * @param name
     */
    public LoadBalancerBuilder<C> withName(String name) {
        this.name = name;
        return this;
    }
    
    /**
     * Strategy used to determine the delay time in msec based on the quarantine 
     * count.  The count is incremented by one for each failure detections and reset
     * once the host is back to normal.
     */
    public LoadBalancerBuilder<C> withQuaratineStrategy(Func1<Integer, Long> quaratineDelayStrategy) {
        this.quaratineDelayStrategy = quaratineDelayStrategy;
        return this;
    }
    
    /**
     * Strategy used to determine how many hosts should be active.
     * This strategy is invoked whenever a host is added or removed from the pool
     */
    public LoadBalancerBuilder<C> withActiveClientCountStrategy(Func1<Integer, Integer> activeClientCountStrategy) {
        this.connectedHostCountStrategy = activeClientCountStrategy;
        return this;
    }
    
    /**
     * Source for host membership events
     */
    public LoadBalancerBuilder<C> withMembershipSource(Observable<MembershipEvent<C>> hostSource) {
        this.hostSource = hostSource;
        return this;
    }
    
    /**
     * Strategy use to calculate weights for active clients
     */
    public LoadBalancerBuilder<C> withWeightingStrategy(WeightingStrategy<C> algorithm) {
        this.weightingStrategy = algorithm;
        return this;
    }
    
    /**
     * Strategy used to select hosts from the calculated weights.  
     * @param selectionStrategy
     */
    public LoadBalancerBuilder<C> withSelectionStrategy(Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy) {
        this.selectionStrategy = selectionStrategy;
        return this;
    }
    
    /**
     * The failure detector returns an Observable that will emit a Throwable for each 
     * failure of the client.  The load balancer will quaratine the client in response.
     * @param failureDetector
     */
    public LoadBalancerBuilder<C> withFailureDetector(FailureDetectorFactory<C> failureDetector) {
        this.failureDetector = failureDetector;
        return this;
    }
    
    /**
     * The connector can be used to prime a client prior to activating it in the connection
     * pool.  
     * @param clientConnector
     */
    public LoadBalancerBuilder<C> withClientConnector(ClientConnector<C> clientConnector) {
        this.clientConnector = clientConnector;
        return this;
    }
    
    public LoadBalancer<C> build() {
        assert hostSource != null;
        
        return new DefaultLoadBalancer<C>(
                name, 
                hostSource, 
                clientConnector, 
                failureDetector, 
                selectionStrategy, 
                quaratineDelayStrategy, 
                connectedHostCountStrategy, 
                weightingStrategy);
    }

    @Override
    public <K> PartitionedLoadBalancerBuilder<C, K> withPartitioner(final Func1<C, Observable<K>> partitioner) {
        return new PartitionedLoadBalancerBuilder<C, K>() {
            @Override
            public PartitionedLoadBalancer<C, K> build() {
                return new DefaultPartitioningLoadBalancer<C, K>(
                        name, 
                        hostSource, 
                        clientConnector, 
                        failureDetector, 
                        selectionStrategy, 
                        quaratineDelayStrategy, 
                        connectedHostCountStrategy, 
                        weightingStrategy, 
                        partitioner);
            }
        };
    }
}
