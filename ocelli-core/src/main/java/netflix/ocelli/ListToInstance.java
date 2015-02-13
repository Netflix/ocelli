package netflix.ocelli;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;

/**
 * Utility class to convert a full list of load balancer members to an Observable<Instance>
 * by diffing the new list with the last list.
 * 
 * @author elandau
 *
 * @param <K>
 * @param <T>
 */
public class ListToInstance<K, T> implements Transformer<List<T>, Instance<T>> {
    
    private final Func1<T, K> keyFunc;

    public ListToInstance(Func1<T, K> keyFunc) {
        this.keyFunc = keyFunc;
    }
    
    @Override
    public Observable<Instance<T>> call(Observable<List<T>> o) {
        return o.flatMap(new Func1<List<T>, Observable<Instance<T>>>() {
            final Map<K, MutableInstance<T>> members = new HashMap<K, MutableInstance<T>>();
            
            @Override
            public Observable<Instance<T>> call(List<T> instances) {
                Map<K, MutableInstance<T>> toRemove = new HashMap<K, MutableInstance<T>>(members);
                List<Instance<T>> newMembers = new ArrayList<Instance<T>>();
                
                for (T ii : instances) {
                    toRemove.remove(keyFunc.call(ii));
                    if (!members.containsKey(keyFunc.call(ii))) {
                        MutableInstance<T> member = MutableInstance.from(ii);
                        members.put(keyFunc.call(ii), member);
                        newMembers.add(member);
                    }
                }
                
                for (Entry<K, MutableInstance<T>> member : toRemove.entrySet()) {
                    members.remove(member.getKey());
                    member.getValue().close();
                }
                
                return Observable.from(newMembers);
            }
        });
    }
}
