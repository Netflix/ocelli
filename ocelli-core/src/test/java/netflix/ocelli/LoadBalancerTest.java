package netflix.ocelli;

import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class LoadBalancerTest {
    @Test
    public void createFromFixedList() {
        LoadBalancer<String> lb = LoadBalancer
            .fromFixedSource(Lists.newArrayList("host1:8080", "host2:8080"))
            .build(RoundRobinLoadBalancer.<String>create(0))
            ;
        
        Assert.assertEquals("host1:8080", lb.next());
        Assert.assertEquals("host2:8080", lb.next());
    }
}
