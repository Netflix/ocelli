package rx.loadbalancer;

import java.util.List;

import rx.functions.Func1;
import rx.loadbalancer.loadbalancer.HostContext;
import rx.loadbalancer.selectors.ClientsAndWeights;

public interface WeightingStrategy<Host, Client, ClientTracker> extends Func1<List<HostContext<Host, Client, ClientTracker>>, ClientsAndWeights<Client>> {

}
