package netflix.ocelli.functions;

import netflix.ocelli.topologies.RingTopology;
import rx.functions.Func1;

/**
 * Convenience class for creating different topologies that filter clients into 
 * a specific arrangement that limit the set of clients this instance will communicate
 * with.
 * 
 * @author elandau
 *
 */
public abstract class Topologies {

    public static <T, K extends Comparable<K>> RingTopology<K, T> ring(K id, Func1<T, K> idFunc, Func1<Integer, Integer> countFunc) {
        return new RingTopology<K, T>(id, idFunc, countFunc);
    }
        
}
