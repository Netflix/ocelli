package netflix.ocelli.loadbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.HostClientConnector;
import netflix.ocelli.HostEvent;
import netflix.ocelli.HostEvent.EventType;
import netflix.ocelli.ManagedClientFactory;
import netflix.ocelli.ManagedLoadBalancer;
import netflix.ocelli.MetricsFactory;
import netflix.ocelli.PartitionedLoadBalancer;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.algorithm.EqualWeightStrategy;
import netflix.ocelli.metrics.CoreClientMetricsFactory;
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
public class DefaultLoadBalancer<H, C> extends AbstractLoadBalancer<H, C> {
    
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancer.class);
    
    /**
     * Da Builder 
     * @author elandau
     *
     * @param <H>
     * @param <C>
     * @param <Tracker>
     */
    public static class Builder<H, C> {
        private Observable<HostEvent<H>>   hostSource;
        private WeightingStrategy<H, C>    weightingStrategy = new EqualWeightStrategy<H, C>();
        private Func1<Integer, Integer>    connectedHostCountStrategy = Functions.identity();
        private Func1<Integer, Long>       quaratineDelayStrategy = Delays.fixed(10, TimeUnit.SECONDS);
        private String                     name = "<unnamed>";
        private Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy = new RoundRobinSelectionStrategy<C>();
        private HostClientConnector<H, C>  connector;
        private List<MetricsFactory<H>>    metricsFactories = new ArrayList<MetricsFactory<H>>();
        private ManagedClientFactory<H, C> clientFactory;
        
        private Builder() {
            metricsFactories.add(new CoreClientMetricsFactory<H>());
        }
        
        public Builder<H, C> withName(String name) {
            this.name = name;
            return this;
        }
        
        public Builder<H, C> withQuaratineStrategy(Func1<Integer, Long> quaratineDelayStrategy) {
            this.quaratineDelayStrategy = quaratineDelayStrategy;
            return this;
        }
        
        public Builder<H, C> withConnectedHostCountStrategy(Func1<Integer, Integer> connectedHostCountStrategy) {
            this.connectedHostCountStrategy = connectedHostCountStrategy;
            return this;
        }
        
        public Builder<H, C> withHostSource(Observable<HostEvent<H>> hostSource) {
            this.hostSource = hostSource;
            return this;
        }
        
        public Builder<H, C> withClientConnector(HostClientConnector<H, C> connector) {
            this.connector = connector;
            return this;
        }
        
        public Builder<H, C> withMetricsFactory(MetricsFactory<H> metricsFactory) {
            this.metricsFactories.add(metricsFactory);
            return this;
        }
        
        public Builder<H, C> withWeightingStrategy(WeightingStrategy<H, C> algorithm) {
            this.weightingStrategy = algorithm;
            return this;
        }
        
        public Builder<H, C> withSelectionStrategy(Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy) {
            this.selectionStrategy = selectionStrategy;
            return this;
        }
        
        Builder<H, C> withClientFactory(ManagedClientFactory<H, C> factory) {
            this.clientFactory = factory;
            return this;
        }
        
        public DefaultLoadBalancer<H, C> build() {
            assert hostSource != null;
            if (this.clientFactory == null) {
                assert connector != null;

                this.clientFactory = new ManagedClientFactory<H, C>(connector, metricsFactories);
            }
            
            return new DefaultLoadBalancer<H, C>(this);
        }
    }
    
    public static <H, C> Builder<H, C> builder() {
        return new Builder<H, C>();
    }
    
    /**
     * Externally provided factory for creating a Client from a Host
     */
    private final ManagedClientFactory<H, C> clientFactory;
    
    /**
     * Source for host membership events
     */
    private final Observable<HostEvent<H>> hostSource;
    
    private DefaultLoadBalancer(Builder<H, C> builder) {
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
    public <I> PartitionedLoadBalancer<H, C, I> partition(Func1<H, Observable<I>> partitioner) {
        return DefaultPartitioningLoadBalancer.<H, C, I>builder()
                .withHostSource(eventStream)
                .withPartitioner(partitioner)
                .withLoadBalancerFactory(new Func2<I,Observable<HostEvent<H>>, ManagedLoadBalancer<H, C>>() {
                    @Override
                    public ManagedLoadBalancer<H, C> call(I id, Observable<HostEvent<H>> hostSource) {
                        LOG.info("Creating partition : " + id);
                        DefaultLoadBalancer<H, C> lb =  DefaultLoadBalancer.<H, C>builder()
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
