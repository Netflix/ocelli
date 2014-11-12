package netflix.ocelli;

public interface ClientAndMetrics<C, M> {
    public C getClient();
    public M getMetrics();
}
