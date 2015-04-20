package netflix.ocelli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import netflix.ocelli.InstanceToNotification.InstanceNotification;
import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * From a list of Instance<T> maintain a List of active T.  Add when T is up and remove
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
    
    @Override
    public Observable<List<T>> call(Observable<Instance<T>> o) {
        return o
            .flatMap(InstanceToNotification.<T>create())
            .scan(new HashSet<T>(), new Func2<HashSet<T>, InstanceNotification<T>, HashSet<T>>() {
                @Override
                public HashSet<T> call(HashSet<T> instances, InstanceNotification<T> notification) {
                    
                    switch (notification.getKind()) {
                    case OnAdd:
                        instances.add(notification.getValue());
                        break;
                    case OnRemove:
                        instances.remove(notification.getValue());
                        break;
                    }
                    return instances;
                }
            })
            .map(new Func1<HashSet<T>, List<T>>() {
                @Override
                public List<T> call(HashSet<T> instances) {
                    // Make an immutable copy of the list
                    return Collections.unmodifiableList(new ArrayList<T>(instances));
                }
            });
    }
}
