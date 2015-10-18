package netflix.ocelli;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to convert a full snapshot of pool members, T, to an Observable<Instance<T>>
 * by diffing the new list with the last list.  Any {@link Instance} that has been removed 
 * will have it's lifecycle terminated.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class SnapshotToInstance<T> implements Transformer<List<T>, Instance<T>> {

    public class State {
        final Map<T, CloseableInstance<T>> members;
        final List<Instance<T>> newInstances;

        public State() {
            members = new HashMap<T, CloseableInstance<T>>();
            newInstances = Collections.emptyList();
        }

        public State(State toCopy) {
            members = new HashMap<T, CloseableInstance<T>>(toCopy.members);
            newInstances = new ArrayList<>();
        }
    }
    
    @Override
    public Observable<Instance<T>> call(Observable<List<T>> snapshots) {
        return snapshots.scan(new State(), new Func2<State, List<T>, State>() {
            @Override
            public State call(State state, List<T> instances) {
                State newState = new State(state);
                Set<T> keysToRemove = new HashSet<T>(newState.members.keySet());

                for (T ii : instances) {
                    keysToRemove.remove(ii);
                    if (!newState.members.containsKey(ii)) {
                        CloseableInstance<T> member = CloseableInstance.from(ii);
                        newState.members.put(ii, member);
                        newState.newInstances.add(member);
                    }
                }

                for (T tKey : keysToRemove) {
                    CloseableInstance<T> removed = newState.members.remove(tKey);
                    if (null != removed) {
                        removed.close();
                    }
                }

                return newState;
            }
        }).concatMap(new Func1<State, Observable<Instance<T>>>() {
            @Override
            public Observable<Instance<T>> call(State state) {
                return Observable.from(state.newInstances);
            }
        });
    }
}
