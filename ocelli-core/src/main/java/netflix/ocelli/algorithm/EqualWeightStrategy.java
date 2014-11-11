package netflix.ocelli.algorithm;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.ManagedClient;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;

/**
 * Strategy where all clients have the same weight
 * @author elandau
 *
 * @param <Host>
 * @param <Client>
 * @param <Metrics>
 */
public class EqualWeightStrategy<Host, Client> implements WeightingStrategy<Host, Client> {

    @Override
    public ClientsAndWeights<Client> call(List<ManagedClient<Host, Client>> t1) {
        List<Client> clients = new ArrayList<Client>(t1.size());
        for (ManagedClient<Host, Client> context : t1) {
            clients.add(context.getClient());
        }
        return new ClientsAndWeights<Client>(clients, null);
    }
}
