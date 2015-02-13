package netflix.ocelli.topologies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import netflix.ocelli.Instance;
import netflix.ocelli.MutableInstance;
import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Action0;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * The ring topology uses consistent hashing to arrange all hosts in a predictable ring 
 * topology such that each client instance will be located in a uniformly distributed
 * fashion around the ring.  The client will then target the next N hosts after it's location.
 * 
 * This type of topology ensures that each client instance communicates with a subset of 
 * hosts in such a manner that the overall load shall be evenly distributed.
 * 
 * @author elandau
 *
 * @param <T>
 * @param <K>
 */

public class RingTopology<K extends Comparable<K>, T> implements Transformer<Instance<T>, Instance<T>> {

    private final Instance<T> localMember;
    private final Func1<Integer, Integer> countFunc;
    private final Comparator<Instance<T>> comparator;
    private final Func1<T, K> keyFunc;
    
    public RingTopology(final K localKey, final Func1<T, K> keyFunc, Func1<Integer, Integer> countFunc) {
        this.localMember = MutableInstance.from((T)null);
        this.countFunc = countFunc;
        this.keyFunc = keyFunc;
        this.comparator = new Comparator<Instance<T>>() {
            @Override
            public int compare(Instance<T> o1, Instance<T> o2) {
                K k1 = o1.getValue() == null ? localKey : keyFunc.call(o1.getValue());
                K k2 = o2.getValue() == null ? localKey : keyFunc.call(o2.getValue());
                return k1.compareTo(k2);
            }
        };
    }
    
    @Override
    public Observable<Instance<T>> call(Observable<Instance<T>> o) {
        return o.flatMap(new Func1<Instance<T>, Observable<Instance<T>>>() {
            final List<Instance<T>> ring = new ArrayList<Instance<T>>();
            Map<K, MutableInstance<T>> members = new HashMap<K, MutableInstance<T>>();
          
            {
                ring.add(localMember);
            }

            @Override
            public Observable<Instance<T>> call(final Instance<T> member) {
                ring.add(member);
                return update().concatWith(member.flatMap(
                        new Func1<Boolean, Observable<Instance<T>>>() {
                            @Override
                            public Observable<Instance<T>> call(Boolean t) {
                                return Observable.empty();
                            }
                        },
                        new Func1<Throwable, Observable<Instance<T>>>() {
                            @Override
                            public Observable<Instance<T>> call(Throwable t1) {
                                ring.remove(t1);
                                return update();
                            }
                        },
                        new Func0<Observable<Instance<T>>>() {
                            @Override
                            public Observable<Instance<T>> call() {
                                ring.remove(member);
                                return update();
                            }
                        }));
            }
            
            private Observable<Instance<T>> update() {
                Collections.sort(ring, comparator);

                // -1 to account for the current instance
                int count = Math.min(ring.size() - 1, countFunc.call(ring.size() - 1));
                List<Instance<T>> toAdd = new ArrayList<Instance<T>>();
                List<MutableInstance<T>> toRemove = new ArrayList<MutableInstance<T>>();
                
                int pos = Collections.binarySearch(ring, localMember, comparator) + 1;
                Map<K, MutableInstance<T>> newMembers = new HashMap<K, MutableInstance<T>>();
                for (int i = 0; i < count; i++) {
                    Instance<T> member = ring.get((pos + i) % ring.size());
                    MutableInstance<T> existing = members.remove(keyFunc.call(member.getValue()));
                    if (existing == null) {
                        MutableInstance<T> newMember = MutableInstance.from(member.getValue());
                        newMembers.put(keyFunc.call(member.getValue()), newMember);
                        toAdd.add(newMember);
                    } 
                    else {
                        newMembers.put(keyFunc.call(member.getValue()), existing);
                    }
                }

                for (MutableInstance<T> member : members.values()) {
                    toRemove.add(member);
                }

                members = newMembers;
                
                return response(toAdd, toRemove);
            }
            
            private Observable<Instance<T>> response(List<Instance<T>> toAdd, final List<MutableInstance<T>> toRemove) {
                return Observable.from(toAdd).doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        for (MutableInstance<T> member : toRemove) {
                            member.close();
                        }
                    }
                });
            }
        });
    }
}
