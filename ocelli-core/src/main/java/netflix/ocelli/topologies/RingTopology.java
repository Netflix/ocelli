package netflix.ocelli.topologies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import netflix.ocelli.CloseableMember;
import netflix.ocelli.Member;
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

public class RingTopology<K extends Comparable<K>, T> implements Transformer<Member<T>, Member<T>> {

    private final Member<T> localMember;
    private final Func1<Integer, Integer> countFunc;
    private final Comparator<Member<T>> comparator;
    private final Func1<T, K> keyFunc;
    
    public RingTopology(final K localKey, final Func1<T, K> keyFunc, Func1<Integer, Integer> countFunc) {
        this.localMember = Member.from(null, Observable.<Void>never());
        this.countFunc = countFunc;
        this.keyFunc = keyFunc;
        this.comparator = new Comparator<Member<T>>() {
            @Override
            public int compare(Member<T> o1, Member<T> o2) {
                K k1 = o1.getValue() == null ? localKey : keyFunc.call(o1.getValue());
                K k2 = o2.getValue() == null ? localKey : keyFunc.call(o2.getValue());
                return k1.compareTo(k2);
            }
        };
    }
    
    @Override
    public Observable<Member<T>> call(Observable<Member<T>> o) {
        return o.flatMap(new Func1<Member<T>, Observable<Member<T>>>() {
            final List<Member<T>> ring = new ArrayList<Member<T>>();
            Map<K, CloseableMember<T>> members = new HashMap<K, CloseableMember<T>>();
          
            {
                ring.add(localMember);
            }

            @Override
            public Observable<Member<T>> call(final Member<T> member) {
                ring.add(member);
                return update().concatWith(member.flatMap(
                        new Func1<Void, Observable<Member<T>>>() {
                            @Override
                            public Observable<Member<T>> call(Void t) {
                                return Observable.empty();
                            }
                        },
                        new Func1<Throwable, Observable<Member<T>>>() {
                            @Override
                            public Observable<Member<T>> call(Throwable t1) {
                                ring.remove(t1);
                                return update();
                            }
                        },
                        new Func0<Observable<Member<T>>>() {
                            @Override
                            public Observable<Member<T>> call() {
                                ring.remove(member);
                                return update();
                            }
                        }));
            }
            
            private Observable<Member<T>> update() {
                Collections.sort(ring, comparator);

                // -1 to account for the current instance
                int count = Math.min(ring.size() - 1, countFunc.call(ring.size() - 1));
                List<Member<T>> toAdd = new ArrayList<Member<T>>();
                List<CloseableMember<T>> toRemove = new ArrayList<CloseableMember<T>>();
                
                int pos = Collections.binarySearch(ring, localMember, comparator) + 1;
                Map<K, CloseableMember<T>> newMembers = new HashMap<K, CloseableMember<T>>();
                for (int i = 0; i < count; i++) {
                    Member<T> member = ring.get((pos + i) % ring.size());
                    CloseableMember<T> existing = members.remove(keyFunc.call(member.getValue()));
                    if (existing == null) {
                        CloseableMember<T> newMember = CloseableMember.from(member.getValue());
                        newMembers.put(keyFunc.call(member.getValue()), newMember);
                        toAdd.add(newMember);
                    } 
                    else {
                        newMembers.put(keyFunc.call(member.getValue()), existing);
                    }
                }

                for (CloseableMember<T> member : members.values()) {
                    toRemove.add(member);
                }

                members = newMembers;
                
                return response(toAdd, toRemove);
            }
            
            private Observable<Member<T>> response(List<Member<T>> toAdd, final List<CloseableMember<T>> toRemove) {
                return Observable.from(toAdd).doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        for (CloseableMember<T> member : toRemove) {
                            member.close();
                        }
                    }
                });
            }
        });
    }
}
