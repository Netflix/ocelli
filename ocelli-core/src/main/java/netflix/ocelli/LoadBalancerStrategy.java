package netflix.ocelli;

import java.util.List;

/**
 * Strategy that when given a list of candidates selects the single best one.
 * @author elandau
 *
 * @param <T>
 */
public interface LoadBalancerStrategy<T> {
    T choose(List<T> candidates);
}
