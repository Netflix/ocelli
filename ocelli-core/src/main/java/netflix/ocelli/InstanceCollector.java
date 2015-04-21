package netflix.ocelli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import netflix.ocelli.InstanceToNotification.InstanceNotification;
import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * From a list of Instance<T> maintain a List of active Instance<T>.  Add when T is up and remove
 * when T is either down or Instance failed or completed.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class InstanceCollector<T extends Instance<?>> implements Transformer<T, List<T>> {
    
    public static <T extends Instance<?>> InstanceCollector<T> create() {
        return new InstanceCollector<T>();
    }
    
    public static <T> Func1<List<Instance<T>>, List<T>> unwrapInstances() {
        return new Func1<List<Instance<T>>, List<T>>() {
            @Override
            public List<T> call(List<Instance<T>> instances) {
                List<T> newList = new ArrayList<T>(instances.size());
                for (Instance<T> instance : instances) {
                    newList.add(instance.getValue());
                }
                return newList;
            }
        };
    }
    
    @Override
    public Observable<List<T>> call(Observable<T> o) {
        return o
            .flatMap(InstanceToNotification.<T>create())
            .scan(new HashSet<T>(), new Func2<Set<T>, InstanceNotification<T>, Set<T>>() {
                @Override
                public Set<T> call(Set<T> instances, InstanceNotification<T> notification) {
                    switch (notification.getKind()) {
                    case OnAdd:
                        instances.add(notification.getInstance());
                        break;
                    case OnRemove:
                        instances.remove(notification.getInstance());
                        break;
                    }
                    return instances;
                }
            })
            .map(new Func1<Set<T>, List<T>>() {
                @Override
                public List<T> call(Set<T> instances) {
                    // Make an immutable copy of the list
                    return Collections.unmodifiableList(new ArrayList<T>(instances));
                }
            });
    }
}
