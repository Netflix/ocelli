package netflix.ocelli.loadbalancer.weighting;

import java.util.List;


public class ClientsAndWeights<C> {
    private final List<C> clients;
    private final List<Integer> weights;
    
    public ClientsAndWeights(List<C> clients, List<Integer> weights) {
        this.clients = clients;
        this.weights = weights;
    }

    public List<C> getClients() {
        return clients;
    }

    public List<Integer> getWeights() {
        return weights;
    }

    public boolean isEmpty() {
        return clients.isEmpty();
    }

    public int size() {
        return clients.size();
    }
    
    public int getTotalWeights() {
        if (weights == null || weights.size() == 0)
            return 0;
        return weights.get(weights.size() -1);
    }

    public C getClient(int index) {
        return clients.get(index);
    }
    
    public int getWeight(int index) {
        if (weights == null) 
            return 0;
        return weights.get(index);
    }
    
    @Override
    public String toString() {
        return "ClientsAndWeights [clients=" + clients + ", weights=" + weights
                + "]";
    }
}
