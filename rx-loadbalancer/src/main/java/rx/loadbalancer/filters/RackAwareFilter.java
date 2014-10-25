package rx.loadbalancer.filters;

import rx.functions.Func1;
import rx.loadbalancer.HostAddress;
import rx.loadbalancer.HostEvent;

/**
 * Filter for selecting only servers that are running on the specified rack
 * 
 * @author elandau
 */
public class RackAwareFilter implements Func1<HostEvent<HostAddress>, Boolean> {
    private String rack;
    
    public RackAwareFilter(String rack) {
        this.rack = rack.toLowerCase();
    }
    
    @Override
    public Boolean call(HostEvent<HostAddress> t1) {
        return t1.getHost().getRack().equals(rack);
    }
}
