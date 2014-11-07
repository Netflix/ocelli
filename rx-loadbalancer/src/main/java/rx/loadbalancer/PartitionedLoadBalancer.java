package rx.loadbalancer;

import rx.Observable;

/**
 * Created from a {@link LoadBalancer} a PartitionedLoadBalancer splits the hosts of 
 * a load balancer across partitions using a partition function.  Note that partitions
 * need not span the entire host space and the same host may exist in multiple partitions.
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
 *  will opt to send traffic only to nodes that can own the data.  The partitioner function
 *  will return the tokenRangeId or shardId for each host.  Note that for replication factor of 1
 *  each shard will contain only 1 host while for higher replication factors each shard will contain
 *  multiple hosts (equal to the number of shards) and that these hosts will overlap.
 *  
 *      <range1> Host1, Host2
 *      <range2> Host2, Host3
 *      <range3> Host3, Host4
 *      <range4> Host4, Host5
 *      
 * @param partitioner   Function that converts a Host into an Observable of partition Ids
 */
public interface PartitionedLoadBalancer<H, C, K> {
    /**
     * @return Return the LoadBalancer for the specified partition key
     */
    public LoadBalancer<H, C> get(K key);

    /**
     * @return Stream of events for this partitioned load balancer
     */
    Observable<HostEvent<H>> events();

    /**
     * @return List all found partition keys.  Note that partition keys are derived from the partitioner only
     */
    Observable<K> listKeys();

    /**
     * 
     */
    public void initialize();
    
    /**
     * 
     */
    public void shutdown();
}
