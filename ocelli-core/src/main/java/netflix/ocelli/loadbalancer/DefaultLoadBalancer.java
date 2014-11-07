package netflix.ocelli.loadbalancer;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.HostClientConnector;
import netflix.ocelli.HostEvent;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.ManagedClientFactory;
import netflix.ocelli.MetricsFactory;
import netflix.ocelli.PartitionedLoadBalancer;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.HostEvent.EventType;
import netflix.ocelli.algorithm.EqualWeightStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;
import netflix.ocelli.selectors.Delays;
import netflix.ocelli.selectors.RoundRobinSelectionStrategy;
import netflix.ocelli.util.Functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * The ClientSelector keeps track of all existing hosts and returns a single host for each
 * call to acquire().
 * 
 * @author elandau
 *
 * @param <H>
 * @param <C>
 * @param <Tracker>
 * 
 * TODO: Host quarantine 
 */
public class DefaultLoadBalancer<H, C, M extends Action1<ClientEvent>> extends AbstractLoadBalancer<H, C, M> {
    
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancer.class);
    
    /**
     * Da Builder 
     * @author elandau
     *
     * @param <H>
     * @param <C>
     * @param <Tracker>
     */
    public static class Builder<H, C, M extends Action1<ClientEvent>> {
        private Observable<HostEvent<H>>   hostSource;
        private WeightingStrategy<H, C, M> weightingStrategy = new EqualWeightStrategy<H, C, M>();
        private Func1<Integer, Integer>    connectedHostCountStrategy = Functions.identity();
        private Func1<Integer, Long>       quaratineDelayStrategy = Delays.fixed(10, TimeUnit.SECONDS);
        private String                     name = "<unnamed>";
        private Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy = new RoundRobinSelectionStrategy<C>();
        private HostClientConnector<H, C> connector;
        private MetricsFactory<H, M> metricsFactory;
        private ManagedClientFactory<H, C, M> clientFactory;
        
        public Builder<H, C, M> withName(String name) {
            this.name = name;
            return this;
        }
        
        public Builder<H, C, M> withQuaratineStrategy(Func1<Integer, Long> quaratineDelayStrategy) {
            this.quaratineDelayStrategy = quaratineDelayStrategy;
            return this;
        }
        
        public Builder<H, C, M> withConnectedHostCountStrategy(Func1<Integer, Integer> connectedHostCountStrategy) {
            this.connectedHostCountStrategy = connectedHostCountStrategy;
            return this;
        }
        
        public Builder<H, C, M> withHostSource(Observable<HostEvent<H>> hostSource) {
            this.hostSource = hostSource;
            return this;
        }
        
        public Builder<H, C, M> withClientConnector(HostClientConnector<H, C> connector) {
            this.connector = connector;
            return this;
        }
        
        public Builder<H, C, M> withMetricsFactory(MetricsFactory<H, M> metricsFactory) {
            this.metricsFactory = metricsFactory;
            return this;
        }
        
        public Builder<H, C, M> withWeightingStrategy(WeightingStrategy<H, C, M> algorithm) {
            this.weightingStrategy = algorithm;
            return this;
        }
        
        public Builder<H, C, M> withSelectionStrategy(Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy) {
            this.selectionStrategy = selectionStrategy;
            return this;
        }
        
        Builder<H, C, M> withClientFactory(ManagedClientFactory<H, C, M> factory) {
            this.clientFactory = factory;
            return this;
        }
        
        public DefaultLoadBalancer<H, C, M> build() {
            assert hostSource != null;
            if (this.clientFactory == null) {
                assert connector != null;
                assert metricsFactory != null;

                this.clientFactory = new ManagedClientFactory<H, C, M>(connector, metricsFactory);
            }
            
            return new DefaultLoadBalancer<H, C, M>(this);
        }
    }
    
    public static <H, C, M extends Action1<ClientEvent>> Builder<H, C, M> builder() {
        return new Builder<H, C, M>();
    }
    
    /**
     * Externally provided factory for creating a Client from a Host
     */
    private final ManagedClientFactory<H, C, M> clientFactory;
    
    /**
     * Source for host membership events
     */
    private final Observable<HostEvent<H>> hostSource;
    
    private DefaultLoadBalancer(Builder<H, C, M> builder) {
        super(
                builder.name, 
                builder.weightingStrategy, 
                builder.connectedHostCountStrategy, 
                builder.quaratineDelayStrategy, 
                builder.selectionStrategy);
        
        this.clientFactory = builder.clientFactory;
        this.hostSource = builder.hostSource;
    }

    public void initialize() {
        super.initialize();
        
        cs.add(hostSource
            .subscribe(new Action1<HostEvent<H>>() {
                @Override
                public void call(HostEvent<H> event) {
                    eventStream.onNext(event);
                    LOG.info("{} :  Got event {}", getName(), event);
                    Holder holder = hosts.get(event.getHost());
                    if (holder == null) {
                        if (event.getType().equals(EventType.ADD)) {
                            final Holder newHolder = new Holder(clientFactory.create(event.getHost()), IDLE);
                            if (null == hosts.putIfAbsent(event.getHost(), newHolder)) {
                                cs.add(newHolder.start());
                            }
                        }
                    }
                    else {
                        holder.sm.call(event.getType());
                    }
                }
            }));
    }

    @Override
    public <I> PartitionedLoadBalancer<H, C, M, I> partition(Func1<H, Observable<I>> partitioner) {
        return DefaultPartitioningLoadBalancer.<H, C, M, I>builder()
                .withHostSource(eventStream)
                .withPartitioner(partitioner)
                .withLoadBalancerFactory(new Func2<I,Observable<HostEvent<H>>, LoadBalancer<H, C, M>>() {
                    @Override
                    public LoadBalancer<H, C, M> call(I id, Observable<HostEvent<H>> hostSource) {
                        LOG.info("Creating partition : " + id);
                        DefaultLoadBalancer<H, C, M> lb =  DefaultLoadBalancer.<H, C, M>builder()
                                .withName(getName() + "_" + id)
                                .withClientFactory(clientFactory)
                                .withHostSource(hostSource)
                                .withQuaratineStrategy(quaratineDelayStrategy)
                                .withSelectionStrategy(selectionStrategy)
                                .withWeightingStrategy(weightingStrategy)
                                .build();
                        lb.initialize();
                        return lb;
                    }
                })
                .build();
    }

}
