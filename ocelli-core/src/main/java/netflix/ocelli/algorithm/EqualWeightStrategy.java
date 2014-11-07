package netflix.ocelli.algorithm;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.ManagedClient;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;
import rx.functions.Action1;

/**
 * Strategy where all clients have the same weight
 * @author elandau
 *
 * @param <Host>
 * @param <Client>
 * @param <Metrics>
 */
public class EqualWeightStrategy<Host, Client, Metrics extends Action1<ClientEvent>> implements WeightingStrategy<Host, Client, Metrics> {

    @Override
    public ClientsAndWeights<Client> call(List<ManagedClient<Host, Client, Metrics>> t1) {
        List<Client> clients = new ArrayList<Client>(t1.size());
        for (ManagedClient<Host, Client, Metrics> context : t1) {
            clients.add(context.getClient());
        }
        return new ClientsAndWeights<Client>(clients, null);
    }
}
