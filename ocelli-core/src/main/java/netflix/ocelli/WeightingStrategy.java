package netflix.ocelli;

import java.util.List;

import netflix.ocelli.selectors.ClientsAndWeights;
import rx.functions.Action1;
import rx.functions.Func1;

public interface WeightingStrategy<Host, Client, Metrics extends Action1<ClientEvent>>
    extends Func1<List<ManagedClient<Host, Client, Metrics>>, ClientsAndWeights<Client>> {
}
