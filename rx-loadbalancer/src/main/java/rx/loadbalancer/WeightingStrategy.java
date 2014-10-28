package rx.loadbalancer;

import java.util.List;

import rx.functions.Func1;
import rx.loadbalancer.loadbalancer.ClientsAndWeights;
import rx.loadbalancer.selector.HostContext;

public interface WeightingStrategy<Host, Client, ClientTracker> extends Func1<List<HostContext<Host, Client, ClientTracker>>, ClientsAndWeights<Client>> {

}
