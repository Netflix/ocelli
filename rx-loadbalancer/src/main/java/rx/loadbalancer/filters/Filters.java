package rx.loadbalancer.filters;

import rx.functions.Func1;
import rx.loadbalancer.HostAddress;
import rx.loadbalancer.HostEvent;

public class Filters {
    public static Func1<HostEvent<HostAddress>, Boolean> inRack(String rack) {
        return new RackAwareFilter(rack);
    }
}
