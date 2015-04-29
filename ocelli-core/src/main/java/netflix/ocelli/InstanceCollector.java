package netflix.ocelli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import rx.Observable;
import rx.Observable.Operator;
import rx.Observable.Transformer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subscriptions.CompositeSubscription;

/**
 * From a list of Instance<T> maintain a List of active Instance<T>.  Add when T is up and remove
 * when T is either down or Instance failed or completed.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class InstanceCollector<T> implements Transformer<Instance<T>, List<T>> {
    
    public static <T> InstanceCollector<T> create() {
        return new InstanceCollector<T>();
    }
    
    // TODO: Move this into a utils package
    public static <K, T> Action1<Instance<T>> toMap(final Map<K, T> map, final Func1<T, K> keyFunc) {
        return new Action1<Instance<T>>() {
            @Override
            public void call(final Instance<T> t1) {
                map.put(keyFunc.call(t1.getValue()), t1.getValue());
                t1.getLifecycle().doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        map.remove(t1.getValue());
                    }
                });
            }
        };
    }
    
    @Override
    public Observable<List<T>> call(Observable<Instance<T>> o) {
        return o.lift(new Operator<Set<T>, Instance<T>>() {
            @Override
            public Subscriber<? super Instance<T>> call(final Subscriber<? super Set<T>> s) {
                final CompositeSubscription cs = new CompositeSubscription();
                final ConcurrentHashMap<T, Subscription> instances = new ConcurrentHashMap<>();
                s.add(cs);
                
                return new Subscriber<Instance<T>>() {
                    @Override
                    public void onCompleted() {
                        s.onCompleted();
                    }

                    @Override
                    public void onError(Throwable e) {
                        s.onError(e);
                    }

                    @Override
                    public void onNext(final Instance<T> t) {
                        Subscription sub = t.getLifecycle().doOnCompleted(new Action0() {
                            @Override
                            public void call() {
                                Subscription sub = instances.remove(t.getValue());
                                cs.remove(sub);
                                s.onNext(instances.keySet());
                            }
                        }).subscribe();
                        
                        instances.put(t.getValue(), sub);
                        
                        s.onNext(instances.keySet());
                    }
                };
            }
        })
        .map(new Func1<Set<T>, List<T>>() {
            @Override
            public List<T> call(Set<T> instances) {
                ArrayList<T> snapshot = new ArrayList<T>(instances.size());
                snapshot.addAll(instances);
              
                // Make an immutable copy of the list
                return Collections.unmodifiableList(snapshot);
            }
        });
    }
}
