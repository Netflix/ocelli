package netflix.ocelli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;
import rx.functions.Func2;

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
    
    public static class State<K, T> {
        final Map<K, MutableInstance<T>> members = new HashMap<K, MutableInstance<T>>();
        List<Instance<T>> newInstances = Collections.emptyList();
    }
    
    @Override
    public Observable<Instance<T>> call(Observable<List<T>> o) {
        return o.scan(new State<K, T>(), new Func2<State<K,T>, List<T>, State<K, T>>() {
            @Override
            public State<K, T> call(State<K, T> state, List<T> instances) {
                Map<K, MutableInstance<T>> toRemove = new HashMap<K, MutableInstance<T>>(state.members);
                state.newInstances = new ArrayList<Instance<T>>();
                
                for (T ii : instances) {
                    toRemove.remove(keyFunc.call(ii));
                    if (!state.members.containsKey(keyFunc.call(ii))) {
                        MutableInstance<T> member = MutableInstance.from(ii);
                        state.members.put(keyFunc.call(ii), member);
                        state.newInstances.add(member);
                    }
                }
                
                for (Entry<K, MutableInstance<T>> member : toRemove.entrySet()) {
                    state.members.remove(member.getKey());
                    member.getValue().close();
                }
                return state;
            }
        })
        .concatMap(new Func1<State<K, T>, Observable<Instance<T>>>() {
            @Override
            public Observable<Instance<T>> call(State<K, T> state) {
                return Observable.from(state.newInstances);
            }
        });
    }
}
