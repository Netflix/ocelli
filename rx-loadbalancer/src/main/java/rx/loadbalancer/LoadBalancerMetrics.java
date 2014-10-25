package rx.loadbalancer;

public interface LoadBalancerMetrics {
    void incrementConnected();
    void decrementConnected();
    void incrementRequest();
    
    int getActiveHost();
    long getRequestCount();
}
