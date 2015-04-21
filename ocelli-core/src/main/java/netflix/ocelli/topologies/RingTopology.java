package netflix.ocelli.topologies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import netflix.ocelli.CloseableInstance;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceToNotification;
import netflix.ocelli.InstanceToNotification.InstanceNotification;
import rx.Observable;
import rx.Observable.Transformer;
import rx.Scheduler;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

/**
 * The ring topology uses consistent hashing to arrange all hosts in a predictable ring 
 * topology such that each client instance will be located in a uniformly distributed
 * fashion around the ring.  The client will then target the next N hosts after its location.
 * 
 * This type of topology ensures that each client instance communicates with a subset of 
 * hosts in such a manner that the overall load shall be evenly distributed.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class RingTopology<K extends Comparable<K>, T> implements Transformer<Instance<T>, Instance<T>> {
    
    private final Func1<Integer, Integer> countFunc;
    private final Func1<T, K> keyFunc;
    private final Entry local;
    
    class Entry implements Comparable<Entry> {
        final K key;
        final T value;
        CloseableInstance<T> instance;
        
        public Entry(K key) {
            this(key, null);
        }
        
        public Entry(K key, T value) {
            this.key = key;
            this.value = value;
        }
        
        @Override
        public int compareTo(Entry o) {
            return key.compareTo(o.key);
        }
        
        public void setInstance(CloseableInstance<T> instance) {
            if (this.instance != null) {
                this.instance.close();
            }
            this.instance = instance;
        }
        
        public void closeInstance() {
            setInstance(null);
        }
        
        public String toString() {
            return key.toString();
        }
    }
    
    class State {
        // Complete hash ordered list of all existing instances
        final List<Entry>   ring = new ArrayList<Entry>();
        
        // Lookup of all entries in the active list
        final Map<K, Entry> active = new HashMap<K, Entry>();
        
        State() {
            ring.add(local);
        }
        
        Observable<Instance<T>> update() {
            Collections.sort(ring);
            
            // Get the starting position in the ring and number of entries
            // that should be active.
            int pos = Collections.binarySearch(ring, local) + 1;
            int count = Math.min(ring.size() - 1, countFunc.call(ring.size() - 1));
            
            // Determine the current 'active' set
            Set<Entry> current = new HashSet<Entry>();
            for (int i = 0; i < count; i++) {
                current.add(ring.get((pos + i) % ring.size()));
            }
            
            // Determine Entries that have either been added or removed
            Set<Entry> added = new HashSet<Entry>(current);
            added.removeAll(active.values());
            
            final Set<Entry> removed = new HashSet<Entry>(active.values());
            removed.removeAll(current);

            // Update the active list
            for (Entry entry : added) {
                active.put(entry.key, entry);
            }
            
            return Observable
                // New instance will be added immediately
                .from(added)
                .map(new Func1<Entry, Instance<T>>() {
                        @Override
                        public Instance<T> call(Entry entry) {
                            CloseableInstance<T> instance = CloseableInstance.from(entry.value);
                            entry.setInstance(instance);
                            return instance;
                        }
                    }
                )
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        for (Entry entry : removed) {
                            entry.closeInstance();
                            active.remove(entry.key);
                        }
                    }
                });
        }
        
        public void add(Instance<T> value) {
            ring.add(new Entry(keyFunc.call(value.getValue()), value.getValue()));
        }
        
        public void remove(Instance<T> value) {
            K key = keyFunc.call(value.getValue());
            int pos = Collections.binarySearch(ring, new Entry(key));
            if (pos >= 0) {
                ring.remove(pos).closeInstance();
                active.remove(key);
            }
        }
    }
    
    public static <K extends Comparable<K>, T> RingTopology<K, T> create(final K localKey, final Func1<T, K> keyFunc, Func1<Integer, Integer> countFunc) {
        return new RingTopology<K, T>(localKey, keyFunc, countFunc);
    }
    
    public static <K extends Comparable<K>, T> RingTopology<K, T> create(final K localKey, final Func1<T, K> keyFunc, Func1<Integer, Integer> countFunc, Scheduler scheduler) {
        return new RingTopology<K, T>(localKey, keyFunc, countFunc, scheduler);
    }
    
    public RingTopology(final K localKey, final Func1<T, K> keyFunc, Func1<Integer, Integer> countFunc) {
        this(localKey, keyFunc, countFunc, Schedulers.computation());
    }
    
    // Visible for testing
    public RingTopology(final K localKey, final Func1<T, K> keyFunc, Func1<Integer, Integer> countFunc, Scheduler scheduler) {
        this.local = new Entry(localKey);
        this.countFunc = countFunc;
        this.keyFunc = keyFunc;
    }
    
    @Override
    public Observable<Instance<T>> call(Observable<Instance<T>> o) {
        return o
            .flatMap(InstanceToNotification.<Instance<T>>create())
            .scan(new State(), new Func2<State, InstanceNotification<Instance<T>>, State>() {
                @Override
                public State call(State state, InstanceNotification<Instance<T>> instance) {
                    switch (instance.getKind()) {
                    case OnAdd:
                        state.add(instance.getInstance());
                        break;
                    case OnRemove:
                        state.remove(instance.getInstance());
                        break;
                    }
                    return state;
                }
            })
            .concatMap(new Func1<State, Observable<Instance<T>>>() {
                @Override
                public Observable<Instance<T>> call(State state) {
                    return state.update();
                }
            });
    }
}
