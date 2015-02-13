package netflix.ocelli;

import rx.Observable;
import rx.functions.Func1;
import rx.observables.GroupedObservable;

/**
 * Representation of a single instance within a pool.  Up/Down state is managed via
 * an Observable<Boolean> where emitting true means the member is active and false
 * means the member is not active (possible due to failure detection).  onCompleted
 * indicates that the PoolMember has been removed.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class Instance<T> extends Observable<Boolean> {
    private final T value;
    
    private static class KeyedInstance<K, T> {
        private final K key;
        private final Instance<T> member;

        KeyedInstance(K key, Instance<T> member) {
            this.key = key;
            this.member = member;
        }
    }
    
    /**
     * Partition Members into multiple partitions based on a partition function.  
     * It's possible for a member to exist in multiple partitions.  Each partition
     * is a GroupedObservable of members for that partition alone.  This stream can 
     * be fed into a load balancer.
     * 
     * Partitions are useful in the following use cases.
     * 
     * 1. Hosts are grouped into VIPs where each VIP subset can service a certain subset of 
     *    requests.  In the example below API's provided by vip1 can be serviced by Hosts 1,2,3
     *    whereas API's provied by vip2 can only be serviced by hosts 2 and 3.
     *    
     *  VIP : F(Host) -> O(vip)   Multiple vips
     *  
     *      <vip1> Host1, Host2, Host3
     *      <vip2> Host2, Host3
     *      
     * 2. Shard or hash aware clients using consistent hashing (ex. Cassandra) or sharding (ex. EvCache)
     *    will opt to send traffic only to nodes that can own the data.  The partitioner function
     *    will return the tokenRangeId or shardId for each host.  Note that for replication factor of 1
     *    each shard will contain only 1 host while for higher replication factors each shard will contain
     *    multiple hosts (equal to the number of shards) and that these hosts will overlap.
     *  
     *      <range1> Host1, Host2
     *      <range2> Host2, Host3
     *      <range3> Host3, Host4
     *      <range4> Host4, Host5
     *
     * @author elandau
     *
     * @param <C>   Client type
     * @param <K>   The partition key
     */
    public static <K, T> Transformer<Instance<T>, GroupedObservable<K, Instance<T>>> partitionBy(final Func1<T, Observable<K>> partitioner) {
        return new Transformer<Instance<T>, GroupedObservable<K, Instance<T>>>() {
            @Override
            public Observable<GroupedObservable<K, Instance<T>>> call(final Observable<Instance<T>> o) {
                return o
                    .flatMap(new Func1<Instance<T>, Observable<KeyedInstance<K, T>>>() {
                        @Override
                        public Observable<KeyedInstance<K, T>> call(final Instance<T> member) {
                            return partitioner
                                    .call(member.getValue())
                                    .map(new Func1<K, KeyedInstance<K, T>>() {
                                        @Override
                                        public KeyedInstance<K, T> call(K key) {
                                            return new KeyedInstance<K, T>(key, member);
                                        }
                                    });
                        }
                    })
                    .groupBy(
                        new Func1<KeyedInstance<K, T>, K>() {
                            @Override
                            public K call(KeyedInstance<K, T> t1) {
                                return t1.key;
                            }
                        }, 
                        new Func1<KeyedInstance<K, T>, Instance<T>>() {
                            @Override
                            public Instance<T> call(KeyedInstance<K, T> t1) {
                                return t1.member;
                            }
                        });
            }
        };
    }

    public Instance(T value, OnSubscribe<Boolean> state) {
        super(state);
        this.value = value;
    }
    
    public T getValue() {
        return this.value;
    }
    
    public String toString() {
        return "Member[" + value + "]";
    }
}
