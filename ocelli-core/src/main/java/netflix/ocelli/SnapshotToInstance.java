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
public class SnapshotToInstance<T> implements Transformer<List<T>, Instance<T>> {
    
    private static <T> Func1<T, Integer> createDefaultHashCode() { 
        return new Func1<T, Integer>() {
            @Override
            public Integer call(T t1) {
                return t1.hashCode();
            }
        };
    }
    
    private static <T> Func2<T, T, Boolean> createDefaultEquals() { 
        return new Func2<T, T, Boolean>() {
            @Override
            public Boolean call(T t1, T t2) {
                return (boolean)t1.equals(t2);
            }
        };
    }
    
    
    private final Func1<T, Integer> hashFunc;
    private final Func2<T, T, Boolean> equalsFunc;

    public SnapshotToInstance() {
        this(SnapshotToInstance.<T>createDefaultHashCode(), SnapshotToInstance.<T>createDefaultEquals());
    }
    
    public SnapshotToInstance(Func1<T, Integer> hashFunc, Func2<T, T, Boolean> equalsFunc) {
        this.hashFunc = hashFunc;
        this.equalsFunc = equalsFunc;
    }
    
    public SnapshotToInstance(final Func1<T, ?> keyFunc) {
        this.hashFunc = new Func1<T, Integer>() {
            @Override
            public Integer call(T t1) {
                return keyFunc.call(t1).hashCode();
            }
        };
        this.equalsFunc = new Func2<T, T, Boolean>() {
            @Override
            public Boolean call(T t1, T t2) {
                return keyFunc.call(t1).equals(t2);
            }
        };
    }
    
    public class TKey {
        private final T obj;

        public TKey(T obj) {
            this.obj = obj;
        }
        
        public int hashCode() {
            return hashFunc.call(obj);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (getClass() != other.getClass())
                return false;
            return equalsFunc.call(obj, ((TKey)other).obj);
        }
    }
    
    public class State {
        final Map<TKey, CloseableInstance<T>> members = new HashMap<TKey, CloseableInstance<T>>();
        List<Instance<T>> newInstances = Collections.emptyList();
    }
    
    @Override
    public Observable<Instance<T>> call(Observable<List<T>> snapshots) {
        return snapshots
            .scan(new State(), new Func2<State, List<T>, State>() {
                @Override
                public State call(State state, List<T> instances) {
                    Map<TKey, CloseableInstance<T>> toRemove = new HashMap<TKey, CloseableInstance<T>>(state.members);
                    state.newInstances = new ArrayList<Instance<T>>();
                    
                    for (T ii : instances) {
                        toRemove.remove(ii);
                        if (!state.members.containsKey(ii)) {
                            CloseableInstance<T> member = CloseableInstance.from(ii);
                            state.members.put(new TKey(ii), member);
                            state.newInstances.add(member);
                        }
                    }
                    
                    for (Entry<TKey, CloseableInstance<T>> member : toRemove.entrySet()) {
                        state.members.remove(member.getKey());
                        member.getValue().close();
                    }
                    return state;
                }
            })
            .concatMap(new Func1<State, Observable<Instance<T>>>() {
                @Override
                public Observable<Instance<T>> call(State state) {
                    return Observable.from(state.newInstances);
                }
            });
    }
}
