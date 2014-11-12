package netflix.ocelli.selectors;

import java.util.List;

public class ClientsAndWeights<Client> {
    private final List<Client> clients;
    private final List<Integer> weights;
    
    public ClientsAndWeights(List<Client> clients, List<Integer> weights) {
        this.clients = clients;
        this.weights = weights;
    }

    public List<Client> getClients() {
        return clients;
    }

    public List<Integer> getWeights() {
        return weights;
    }

    public boolean isEmpty() {
        return clients.isEmpty();
    }

    public int getTotalWeights() {
        return weights.get(weights.size()-1);
    }

    @Override
    public String toString() {
        return "ClientsAndWeights [clients=" + clients + ", weights=" + weights
                + "]";
    }
}
