package rx.loadbalancer.loadbalancer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.loadbalancer.ClientEvent;
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.LoadBalancer;
import rx.loadbalancer.PartitionedLoadBalancer;
import rx.subjects.PublishSubject;
import rx.subscriptions.CompositeSubscription;

public class DefaultPartitioningLoadBalancer<H, C, M extends Action1<ClientEvent>, K> implements PartitionedLoadBalancer<H, C, K> {
    public static class Builder<H, C, I, T extends Action1<ClientEvent>> {
        private Func1<H, Observable<I>> partitioner;
        private Observable<HostEvent<H>> hostSource;
        private Func2<I, Observable<HostEvent<H>>, LoadBalancer<H, C>> factory;
        
        public Builder<H, C, I, T> withHostSource(Observable<HostEvent<H>> hosts) {
            this.hostSource = hosts;
            return this;
        }
        
        public Builder<H, C, I, T> withPartitioner(Func1<H, Observable<I>> partitioner) {
            this.partitioner = partitioner;
            return this;
        }
        
        public Builder<H, C, I, T> withLoadBalancerFactory(Func2<I, Observable<HostEvent<H>>, LoadBalancer<H, C>> factory) {
            this.factory = factory;
            return this;
        }
        
        public DefaultPartitioningLoadBalancer<H, C, T, I> build() {
            return new DefaultPartitioningLoadBalancer<H, C, T, I>(this);
        }
    }
    
    public static <H, C, M extends Action1<ClientEvent>, I> Builder<H, C, I, M> builder() {
        return new Builder<H, C, I, M>();
    }

    private final CompositeSubscription cs = new CompositeSubscription();
    private final Func1<H, Observable<K>> partitioner;
    private final Func2<K, Observable<HostEvent<H>>, LoadBalancer<H, C>> factory;
    private final Observable<HostEvent<H>> hostSource;
    private final PublishSubject<HostEvent<H>> eventStream = PublishSubject.create();
    private final ConcurrentMap<K, Holder> partitions = new ConcurrentHashMap<K, Holder>();
    
    private final class Holder {
        final PublishSubject<HostEvent<H>> hostStream;
        final LoadBalancer<H, C> loadBalancer;
        
        public Holder(LoadBalancer<H, C> loadBalancer, PublishSubject<HostEvent<H>> hostStream) {
            this.loadBalancer = loadBalancer;
            this.hostStream = hostStream;
        }
    }
    
    private DefaultPartitioningLoadBalancer(Builder<H, C, K, M> builder) {
        this.partitioner = builder.partitioner;
        this.hostSource  = builder.hostSource;
        this.factory     = builder.factory;
    }
    
    @Override
    public void initialize() {
        cs.add(hostSource
            .subscribe(eventStream));
        
        cs.add(hostSource
            .subscribe(new Action1<HostEvent<H>>() {
                @Override
                public void call(final HostEvent<H> event) {
                    partitioner
                            .call(event.getHost())
                            .subscribe(new Action1<K>() {
                                @Override
                                public void call(K id) {
                                    Holder holder = partitions.get(id);
                                    if (null == holder) {
                                        PublishSubject<HostEvent<H>> subject = PublishSubject.create();
                                        holder = new Holder(factory.call(id, subject), subject);
                                        Holder prev = partitions.putIfAbsent(id, holder);
                                        if (prev != null) {
                                            holder = prev;
                                        }
                                    }
                                    holder.hostStream.onNext(event);
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
    
    @Override
    public LoadBalancer<H, C> get(K id) {
        Holder holder = partitions.get(id);
        if (holder == null)
            return null;
        
        return holder.loadBalancer;
    }

    @Override
    public Observable<HostEvent<H>> events() {
        return eventStream;
    }
    
    @Override
    public Observable<K> listKeys() {
        return Observable.from(partitions.keySet());
    }
}
