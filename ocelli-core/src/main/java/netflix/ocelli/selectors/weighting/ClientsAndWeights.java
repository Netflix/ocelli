package netflix.ocelli.selectors.weighting;


public class ClientsAndWeights<C> {
    private final C[] clients;
    private final int[] weights;
    
    public ClientsAndWeights(C[] clients, int[] weights) {
        this.clients = clients;
        this.weights = weights;
    }

    public C[] getClients() {
        return clients;
    }

    public int[] getWeights() {
        return weights;
    }

    public boolean isEmpty() {
        return clients.length == 0;
    }

    public int size() {
        return clients.length;
    }
    
    public int getTotalWeights() {
        return weights[weights.length-1];
    }

    public C getClient(int index) {
        return clients[index];
    }
    
    public int getWeight(int index) {
        assert(weights.length > 0);
        
        return weights[index];
    }
    
    @Override
    public String toString() {
        return "ClientsAndWeights [clients=" + clients + ", weights=" + weights
                + "]";
    }
}
