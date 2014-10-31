package rx.loadbalancer.algorithm;

import java.util.ArrayList;
import java.util.List;

import rx.loadbalancer.WeightingStrategy;
import rx.loadbalancer.loadbalancer.HostContext;
import rx.loadbalancer.selectors.ClientsAndWeights;

/**
 * Strategy where all clients have the same weight
 * @author elandau
 *
 * @param <Host>
 * @param <Client>
 * @param <Tracker>
 */
public class EqualWeightStrategy<Host, Client, Tracker> implements WeightingStrategy<Host, Client, Tracker> {

    @Override
    public ClientsAndWeights<Client> call(List<HostContext<Host, Client, Tracker>> t1) {
        List<Client> clients = new ArrayList<Client>(t1.size());
        for (HostContext<Host, Client, Tracker> context : t1) {
            clients.add(context.getClient());
        }
        return new ClientsAndWeights<Client>(clients, null);
    }
}
