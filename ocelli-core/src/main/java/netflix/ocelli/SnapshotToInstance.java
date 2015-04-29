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
 * Utility class to convert a full snapshot of pool members, T, to an Observable<Instance<T>>
 * by diffing the new list with the last list.  Any {@link Instance} that has been removed 
 * will have it's lifecycle terminated.
 * 
 * @author elandau
 *
 * @param <K>
 * @param <T>
 */
public class SnapshotToInstance<K, T> implements Transformer<List<T>, Instance<T>> {
    
    private final Func1<T, K> keyFunc;

    public SnapshotToInstance(Func1<T, K> keyFunc) {
        this.keyFunc = keyFunc;
    }
    
    public static class State<K, T> {
        final Map<K, CloseableInstance<T>> members = new HashMap<K, CloseableInstance<T>>();
        List<Instance<T>> newInstances = Collections.emptyList();
    }
    
    @Override
    public Observable<Instance<T>> call(Observable<List<T>> snapshots) {
        return snapshots
            .scan(new State<K, T>(), new Func2<State<K,T>, List<T>, State<K, T>>() {
                @Override
                public State<K, T> call(State<K, T> state, List<T> instances) {
                    Map<K, CloseableInstance<T>> toRemove = new HashMap<K, CloseableInstance<T>>(state.members);
                    state.newInstances = new ArrayList<Instance<T>>();
                    
                    for (T ii : instances) {
                        toRemove.remove(keyFunc.call(ii));
                        if (!state.members.containsKey(keyFunc.call(ii))) {
                            CloseableInstance<T> member = CloseableInstance.from(ii);
                            state.members.put(keyFunc.call(ii), member);
                            state.newInstances.add(member);
                        }
                    }
                    
                    for (Entry<K, CloseableInstance<T>> member : toRemove.entrySet()) {
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
