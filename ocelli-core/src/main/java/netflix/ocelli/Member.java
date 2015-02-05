package netflix.ocelli;

import rx.Observable;
import rx.Subscriber;
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
public class Member<T> extends Observable<Void> {
    private final T value;
    
    private static class PartitionedMember<K, T> {
        private final K key;
        private final Member<T> member;

        PartitionedMember(K key, Member<T> member) {
            this.key = key;
            this.member = member;
        }
    }
    
    public static <T> Member<T> from(T value, Observable<Void> shutdown) {
        return new Member<T>(value, shutdown);
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
    public static <K, T> Transformer<Member<T>, GroupedObservable<K, Member<T>>> partitionBy(final Func1<T, Observable<K>> partitioner) {
        return new Transformer<Member<T>, GroupedObservable<K, Member<T>>>() {
            @Override
            public Observable<GroupedObservable<K, Member<T>>> call(final Observable<Member<T>> o) {
                return o
                    .flatMap(new Func1<Member<T>, Observable<PartitionedMember<K, T>>>() {
                        @Override
                        public Observable<PartitionedMember<K, T>> call(final Member<T> member) {
                            return partitioner
                                    .call(member.getValue())
                                    .map(new Func1<K, PartitionedMember<K, T>>() {
                                        @Override
                                        public PartitionedMember<K, T> call(K key) {
                                            return new PartitionedMember<K, T>(key, member);
                                        }
                                    });
                        }
                    })
                    .groupBy(
                        new Func1<PartitionedMember<K, T>, K>() {
                            @Override
                            public K call(PartitionedMember<K, T> t1) {
                                return t1.key;
                            }
                        }, 
                        new Func1<PartitionedMember<K, T>, Member<T>>() {
                            @Override
                            public Member<T> call(PartitionedMember<K, T> t1) {
                                return t1.member;
                            }
                        });
            }
        };
    }

    public Member(T value, final Observable<Void> shutdown) {
        super(new OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> t1) {
                shutdown.subscribe(t1);
            }
        });
        this.value = value;
    }
    
    public T getValue() {
        return this.value;
    }
    
    public String toString() {
        return "Member[" + value + "]";
    }
}
