package netflix.ocelli.loadbalancer.weighting;

import rx.functions.Func1;

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
            
    public static Func1<IntClientAndMetrics, Integer> BY_METRIC = new Func1<IntClientAndMetrics, Integer>() {
        @Override
        public Integer call(IntClientAndMetrics t1) {
            return t1.getMetrics();
        }            
    };

    @Override
    public String toString() {
        return "IntClientAndMetrics [client=" + client + ", metrics=" + metrics + "]";
    }
    
}
