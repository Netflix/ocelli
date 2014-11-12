package netflix.ocelli.loadbalancer;

import netflix.ocelli.ClientConnector;
import netflix.ocelli.FailureDetectorFactory;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MetricsFactory;
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

import java.util.concurrent.TimeUnit;

/**
 * A builder to create instances of {@link LoadBalancer}.
 *
 * @author elandau
 *
 * @param <C>
 * @param <M>
 */
public class LoadBalancerBuilder<C, M> {

    Observable<MembershipEvent<C>> hostSource;
    String                      name = "<unnamed>";
    WeightingStrategy<C, M> weightingStrategy = new EqualWeightStrategy<C, M>();
    Func1<Integer, Integer> connectedHostCountStrategy = Functions.identity();
    Func1<Integer, Long>        quaratineDelayStrategy = Delays.fixed(10, TimeUnit.SECONDS);
    Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy = new RoundRobinSelectionStrategy<C>();
    FailureDetectorFactory<C> failureDetector = Failures.never();
    ClientConnector<C> clientConnector = Connectors.immediate();
    Func1<C, Observable<M>>     metricsMapper;

    LoadBalancerBuilder() {
    }

    public LoadBalancerBuilder(Observable<MembershipEvent<C>> hostSource, MetricsFactory<C, M> metricsMapper) {
        this.hostSource = hostSource;
        this.metricsMapper = metricsMapper;
    }

    /**
     *
     * Arbitrary name assigned to the connection pool, mostly for debugging purposes
     * @param name
     */
    public LoadBalancerBuilder<C, M> withName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Strategy used to determine the delay time in msec based on the quarantine
     * count.  The count is incremented by one for each failure detections and reset
     * once the host is back to normal.
     */
    public LoadBalancerBuilder<C, M> withQuarantineStrategy(Func1<Integer, Long> quaratineDelayStrategy) {
        this.quaratineDelayStrategy = quaratineDelayStrategy;
        return this;
    }

    /**
     * Strategy used to determine how many hosts should be active.
     * This strategy is invoked whenever a host is added or removed from the pool
     */
    public LoadBalancerBuilder<C, M> withActiveClientCountStrategy(Func1<Integer, Integer> activeClientCountStrategy) {
        this.connectedHostCountStrategy = activeClientCountStrategy;
        return this;
    }

    /**
     * Source for host membership events
     */
    public LoadBalancerBuilder<C, M> withMembershipSource(Observable<MembershipEvent<C>> hostSource) {
        this.hostSource = hostSource;
        return this;
    }

    /**
     * Strategy use to calculate weights for active clients
     */
    public LoadBalancerBuilder<C, M> withWeightingStrategy(WeightingStrategy<C, M> algorithm) {
        this.weightingStrategy = algorithm;
        return this;
    }

    /**
     * Strategy used to select hosts from the calculated weights.
     * @param selectionStrategy
     */
    public LoadBalancerBuilder<C, M> withSelectionStrategy(Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy) {
        this.selectionStrategy = selectionStrategy;
        return this;
    }

    /**
     * The failure detector returns an Observable that will emit a Throwable for each
     * failure of the client.  The load balancer will quaratine the client in response.
     * @param failureDetector
     */
    public LoadBalancerBuilder<C, M> withFailureDetector(FailureDetectorFactory<C> failureDetector) {
        this.failureDetector = failureDetector;
        return this;
    }

    /**
     * The connector can be used to prime a client prior to activating it in the connection
     * pool.
     * @param clientConnector
     */
    public LoadBalancerBuilder<C, M> withClientConnector(ClientConnector<C> clientConnector) {
        this.clientConnector = clientConnector;
        return this;
    }

    /**
     * Factory for creating and associating the metrics used for weighting with a client.
     * Note that for robust client interface C and M may be the same client type and the
     * factory will simply return an Observable.just(client);
     *
     * @param metricsMapper
     * @return
     */
    public LoadBalancerBuilder<C, M> withMetricsFactory(MetricsFactory<C, M> metricsMapper) {
        this.metricsMapper = metricsMapper;
        return this;
    }

    public DefaultLoadBalancer<C, M> build() {
        assert hostSource != null;
        assert metricsMapper != null;

        return new DefaultLoadBalancer<C, M>(this);
    }
}
