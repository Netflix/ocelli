package netflix.ocelli;

/**
 * Strategy to determine the backoff delay after N consecutive failures.
 * 
 * @author elandau
 *
 */
public interface DelayStrategy {
    long get(int consecutiveFailures);
}
