package rx.loadbalancer.algorithm;

import java.util.List;

import rx.loadbalancer.WeightingStrategy;
import rx.loadbalancer.loadbalancer.ClientsAndWeights;
import rx.loadbalancer.selector.HostContext;

public class LeastLoadedAlgorithm<Host, Client, Tracker> implements WeightingStrategy<Host, Client, Tracker> {

    @Override
    public ClientsAndWeights<Client> call(List<HostContext<Host, Client, Tracker>> t1) {
        // TODO Auto-generated method stub
        return null;
    }

}
