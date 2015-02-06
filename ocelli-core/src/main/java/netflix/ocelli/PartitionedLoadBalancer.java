package netflix.ocelli;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observables.GroupedObservable;

/**
 * Observer to a partitioned source of Member instances which collects the paritions and creates
 * a matching load balancer for each.
 * 
 * @author elandau
 *
 * @param <K>
 * @param <T>
 */
public class PartitionedLoadBalancer<K, T> extends LoadBalancer<T> implements Observer<GroupedObservable<K, Instance<T>>> {
    private ConcurrentMap<K, LoadBalancer<T>> lbs = new ConcurrentHashMap<K, LoadBalancer<T>>();
    
    private final Func1<Observable<List<T>>, LoadBalancer<T>> loadBalancerFactory;
    
    public PartitionedLoadBalancer() {
        this(new Func1<Observable<List<T>>, LoadBalancer<T>>() {
            @Override
            public LoadBalancer<T> call(Observable<List<T>> instances) {
                return RoundRobinLoadBalancer.from(instances);
            }
        });
    }
    
    public PartitionedLoadBalancer(Func1<Observable<List<T>>, LoadBalancer<T>> loadBalancerFactory) {
        this.loadBalancerFactory = loadBalancerFactory;
    }
    
    @Override
    public void onCompleted() {
        // OK to ignore
    }

    @Override
    public void onError(Throwable e) {
        // OK to ignore
    }

    @Override
    public void onNext(GroupedObservable<K, Instance<T>> t) {
        LoadBalancer<T> lb = loadBalancerFactory.call(t.compose(new InstanceCollector<T>()));
        lbs.putIfAbsent(t.getKey(), lb);
    }

    @Override
    public void call(Subscriber<? super T> t1) {
        // TODO: 
    }

    @Override
    public void shutdown() {
        for (LoadBalancer<T> lb : lbs.values()) {
            lb.shutdown();
        }
    }

    @Override
    public Observable<T> all() {
        Observable<T> o = Observable.empty();
        for (LoadBalancer<T> lb : lbs.values()) {
            o.concatWith(lb.all());
        }
        return o;
    }
    
    public LoadBalancer<T> get(final K key) {
        LoadBalancer<T> lb = lbs.get(key);
        if (lb != null) {
            return lb;
        }
        
        return new LoadBalancer<T>() {
            @Override
            public void call(Subscriber<? super T> t1) {
                LoadBalancer<T> lb = lbs.get(key);
                if (lb != null) {
                    lb.call(t1);
                }
                else {
                    t1.onError(new NoSuchElementException());
                }
            }

            @Override
            public void shutdown() {
            }

            @Override
            public Observable<T> all() {
                LoadBalancer<T> lb = lbs.get(key);
                if (lb != null) {
                    return lb.all();
                }
                else {
                    return Observable.empty();
                }
            }
        };
    }
}
