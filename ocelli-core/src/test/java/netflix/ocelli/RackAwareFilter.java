package netflix.ocelli;

import netflix.ocelli.MembershipEvent;
import rx.functions.Func1;

/**
 * Filter for selecting only servers that are running on the specified rack
 * 
 * @author elandau
 */
public class RackAwareFilter implements Func1<MembershipEvent<HostAddress>, Boolean> {
    private String rack;
    
    public RackAwareFilter(String rack) {
        this.rack = rack.toLowerCase();
    }
    
    @Override
    public Boolean call(MembershipEvent<HostAddress> t1) {
        return t1.getClient().getRack().equals(rack);
    }
}
