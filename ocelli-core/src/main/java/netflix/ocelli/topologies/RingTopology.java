package netflix.ocelli.topologies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import rx.Observable.Operator;
import rx.Subscriber;
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
public class RingTopology<T, K extends Comparable<K>> implements Operator<MembershipEvent<T>, MembershipEvent<T>> {
    
    private static class Holder<T, K extends Comparable<K>> implements Comparable<Holder<T, K>>{
        private final T value;
        private final K key;
        
        Holder(T value, K key) {
            assert key != null;
            this.value = value;
            this.key = key;
        }

        @Override
        public int compareTo(Holder<T, K> o) {
            assert key != null;
            if (o == null)
                return 0;
            return key.compareTo(o.key);
        }
        
        public String toString() {
            return key.toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Holder other = (Holder) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            return true;
        }
    }
    
    private final Func1<T, K> hashFunc;
    private final Holder<T, K> me;
    private Func1<Integer, Integer> countFunc;
    
    public RingTopology(K key, Func1<T, K> hashFunc, Func1<Integer, Integer> countFunc) {
        this.hashFunc  = hashFunc;
        this.countFunc = countFunc;
        this.me        = new Holder<T, K>(null, key);
    }
    
    @Override
    public Subscriber<? super MembershipEvent<T>> call(final Subscriber<? super MembershipEvent<T>> subscriber) {
        return new Subscriber<MembershipEvent<T>>(subscriber) {
            final ReentrantLock lock = new ReentrantLock();
            final List<Holder<T, K>> ring = new ArrayList<Holder<T, K>>();
            
            Set<Holder<T, K>> members = new HashSet<Holder<T, K>>();
            
            {
                ring.add(me);
            }
            
            @Override
            public void onCompleted() {
                subscriber.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                subscriber.onError(e);
            }

            @Override
            public void onNext(final MembershipEvent<T> t) {
                try {
                    lock.lock();
                    
                    // Update the ring
                    switch (t.getType()) {
                    case ADD:
                        ring.add(new Holder<T, K>(t.getClient(), hashFunc.call(t.getClient())));
                        break;
                    case REMOVE:
                        ring.remove(new Holder<T, K>(t.getClient(), hashFunc.call(t.getClient())));
                        break;
                    }
                    
                    // Re-sort the ring
                    Collections.sort(ring);
                    
                    // -1 to account for 'me'
                    int count = Math.min(ring.size()-1, countFunc.call(ring.size()-1));
                    
                    // Determine the new set of hosts
                    int pos = Collections.binarySearch(ring, me)+1;
                    Set<Holder<T, K>> newMembers = new HashSet<Holder<T, K>>();
                    for (int i = 0; i < count; i++) {
                        newMembers.add(ring.get((pos + i) % ring.size()));
                    }
                    
                    // Forward 'new' hosts as ADD events
                    for (Holder<T, K> member : newMembers) {
                        if (!members.contains(member)) {
                            subscriber.onNext(MembershipEvent.create(member.value, EventType.ADD));
                        }
                    }
                    
                    // Forward 'removed' hosts as REMOVE events 
                    for (final Holder<T, K> member : members) {
                        if (!newMembers.contains(member)) {
                            subscriber.onNext(MembershipEvent.create(member.value, EventType.REMOVE));
                        }
                    }
                    
                    members = newMembers;
                }
                finally {
                    lock.unlock();
                }
            }
        };
    }
}
