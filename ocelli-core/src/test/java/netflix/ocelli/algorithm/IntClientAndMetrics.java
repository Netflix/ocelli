package netflix.ocelli.algorithm;

public class IntClientAndMetrics {
    private Integer client;
    private Integer metrics;
    
    public IntClientAndMetrics(int client, int metrics) {
        this.client = client;
        this.metrics = metrics;
    }
    
    public Integer getClient() {
        return client;
    }

    public Integer getMetrics() {
        return metrics;
    }
}
