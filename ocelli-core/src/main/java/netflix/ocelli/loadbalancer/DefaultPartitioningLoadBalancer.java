package netflix.ocelli.loadbalancer;

import netflix.ocelli.ClientConnector;
import netflix.ocelli.FailureDetectorFactory;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.LoadBalancers;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.PartitionedLoadBalancer;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import rx.subscriptions.CompositeSubscription;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultPartitioningLoadBalancer<C, K> implements PartitionedLoadBalancer<C, K> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultPartitioningLoadBalancer.class);
    
    private final CompositeSubscription cs = new CompositeSubscription();
    private final Func1<C, Observable<K>> partitioner;
    private final Observable<MembershipEvent<C>> hostSource;
    private final ConcurrentMap<K, Holder> partitions = new ConcurrentHashMap<K, Holder>();
    private final WeightingStrategy<C> weightingStrategy;
    private final FailureDetectorFactory<C> failureDetector;
    private final ClientConnector<C> clientConnector;
    private final Func1<Integer, Integer> connectedHostCountStrategy;
    private final Func1<Integer, Long> quaratineDelayStrategy;
    private final Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy;
    private final String name;
    
    private final class Holder {
        final PublishSubject<MembershipEvent<C>> hostStream;
        final LoadBalancer<C> loadBalancer;
        
        public Holder(LoadBalancer<C> loadBalancer, PublishSubject<MembershipEvent<C>> hostStream) {
            this.loadBalancer = loadBalancer;
            this.hostStream = hostStream;
        }
    }
    
    public DefaultPartitioningLoadBalancer(
            String name,
            Observable<MembershipEvent<C>> hostSource,
            ClientConnector<C> clientConnector,
            FailureDetectorFactory<C> failureDetector,
            Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy,
            Func1<Integer, Long> quaratineDelayStrategy,
            Func1<Integer, Integer> connectedHostCountStrategy,
            WeightingStrategy<C> weightingStrategy,
            Func1<C, Observable<K>> partitioner) {
        
        this.partitioner            = partitioner;
        this.hostSource             = hostSource;
        this.failureDetector        = failureDetector;
        this.clientConnector        = clientConnector;
        this.selectionStrategy      = selectionStrategy;
        this.weightingStrategy      = weightingStrategy;
        this.quaratineDelayStrategy = quaratineDelayStrategy;
        this.name                   = name;
        this.connectedHostCountStrategy = connectedHostCountStrategy;
        
        initialize();
    }

    private void initialize() {
        cs.add(hostSource
            .subscribe(new Action1<MembershipEvent<C>>() {
                @Override
                public void call(final MembershipEvent<C> event) {
                    partitioner
                            .call(event.getClient())
                            .subscribe(new Action1<K>() {
                                @Override
                                public void call(K id) {
                                    getOrCreateHolder(id).hostStream.onNext(event);
                                }
                            });
                }
            })
        );
    }
    
    @Override
    public void shutdown() {
        cs.unsubscribe();
    }
    
    private Holder getOrCreateHolder(K id) {
        Holder holder = partitions.get(id);
        if (null == holder) {
            PublishSubject<MembershipEvent<C>> subject = PublishSubject.create();
            Holder newHolder = new Holder(createPartition(id, subject), subject);
            holder = partitions.putIfAbsent(id, newHolder);
            if (holder == null) {
                holder = newHolder;
            }
        }
        return holder;
    }
    
    @Override
    public LoadBalancer<C> get(K id) {
        return getOrCreateHolder(id).loadBalancer;
    }

    @Override
    public Observable<K> listKeys() {
        return Observable.from(partitions.keySet());
    }
    
    private LoadBalancer<C> createPartition(K id, Observable<MembershipEvent<C>> hostSource) {
        LOG.info("Creating partition : " + id);
        return LoadBalancers.newBuilder(hostSource)
                .withName(getName() + "_" + id)
                .withQuarantineStrategy(quaratineDelayStrategy)
                .withSelectionStrategy(selectionStrategy)
                .withWeightingStrategy(weightingStrategy)
                .withActiveClientCountStrategy(connectedHostCountStrategy)
                .withClientConnector(clientConnector)
                .withFailureDetector(failureDetector)
                .build();
    }

    private String getName() {
        return this.name;
    }
}
