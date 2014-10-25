package rx.loadbalancer.loadbalancer;


public class ClientsWithWeights<Client> {
    private final Client[] clients;
    private final Integer[] weights;
    
    public ClientsWithWeights(Client[] clients, Integer[] weights) {
        this.clients = clients;
        this.weights = weights;
    }

    public Client[] getClients() {
        return clients;
    }

    public Integer[] getWeights() {
        return weights;
    }

    public boolean isEmpty() {
        return clients.length == 0;
    }

    public int getTotalWeights() {
        return weights[weights.length-1];
    }
}
