package rx.loadbalancer;

import java.util.List;

import rx.functions.Action1;
import rx.functions.Func1;
import rx.loadbalancer.selectors.ClientsAndWeights;

public interface WeightingStrategy<Host, Client, Metrics extends Action1<ClientEvent>>
    extends Func1<List<ManagedClient<Host, Client, Metrics>>, ClientsAndWeights<Client>> {
}
