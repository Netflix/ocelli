package netflix.ocelli;

import netflix.ocelli.MembershipEvent;
import rx.functions.Func1;

public class Filters {
    public static Func1<MembershipEvent<HostAddress>, Boolean> inRack(String rack) {
        return new RackAwareFilter(rack);
    }
}
