package netflix.ocelli;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

/**
 * Partition incoming MembershipEvents to multiple partitions based on a partition
 * function.  It's possible for a host to exist in multiple partitions.  Each partition
 * is an observable of MembershipEvent for that partition alone.  This stream can 
 * be fed into a load balancer selector
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
public class MembershipPartitioner<C, K> {
    private final ConcurrentMap<K, PublishSubject<MembershipEvent<C>>> partitions = new ConcurrentHashMap<K, PublishSubject<MembershipEvent<C>>>();
    private final Subscription s;
    
    
    public static <C, K> MembershipPartitioner<C, K> create(Observable<MembershipEvent<C>> source, final Func1<C, Observable<K>> partitioner) {
        return new MembershipPartitioner<C, K>(source, partitioner);
    }
    
    /**
     * 
     * @param partitioner   Function that converts a Host into an Observable of partition Ids
     */
    public MembershipPartitioner(Observable<MembershipEvent<C>> source, final Func1<C, Observable<K>> partitioner) {
        s = source.subscribe(new Action1<MembershipEvent<C>>() {
                @Override
                public void call(final MembershipEvent<C> event) {
                    partitioner.call(event.getClient())
                    .subscribe(new Action1<K>() {
                        @Override
                        public void call(K t1) {
                            getOrCreateGroup(t1).onNext(event);
                        }
                    });
                }
            });
    }
    
    public void shutdown() {
        s.unsubscribe();
    }
    
    private PublishSubject<MembershipEvent<C>> getOrCreateGroup(K id) {
        PublishSubject<MembershipEvent<C>> group = partitions.get(id);
        if (null == group) {
            group = PublishSubject.<MembershipEvent<C>>create();
            PublishSubject<MembershipEvent<C>> existing = partitions.putIfAbsent(id, group);
            if (existing != null) {
                group = existing;
            }
        }
        return group;
    }
    
    /**
     * Return the membership stream for the partition at the request id.  A new blank partition
     * will be created if membership for the partition hasn't been created yet.  
     * @param id
     * @return
     */
    public Observable<MembershipEvent<C>> getPartition(K id) {
        return getOrCreateGroup(id);
    }

    /**
     * List all known partition keys
     * @return
     */
    public Observable<K> listKeys() {
        return Observable.from(partitions.keySet());
    }
}
