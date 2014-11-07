package netflix.ocelli.filters;

import netflix.ocelli.HostAddress;
import netflix.ocelli.HostEvent;
import rx.functions.Func1;

public class Filters {
    public static Func1<HostEvent<HostAddress>, Boolean> inRack(String rack) {
        return new RackAwareFilter(rack);
    }
}
