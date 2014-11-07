package rx.loadbalancer.algorithm;

import java.util.ArrayList;
import java.util.List;

import rx.functions.Action1;
import rx.loadbalancer.ClientEvent;
import rx.loadbalancer.ManagedClient;
import rx.loadbalancer.WeightingStrategy;
import rx.loadbalancer.selectors.ClientsAndWeights;

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
