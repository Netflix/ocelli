package netflix.ocelli.loadbalancer;

import java.util.NoSuchElementException;

import junit.framework.Assert;
import netflix.ocelli.LoadBalancer;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class FallbackLoadBalancerTest {
    private static SettableLoadBalancer<Integer> lb1 = RoundRobinLoadBalancer.create();
    private static SettableLoadBalancer<Integer> lb2 = RoundRobinLoadBalancer.create();
    private static SettableLoadBalancer<Integer> lb3 = RoundRobinLoadBalancer.create();
    
    @BeforeClass
    public static void before() {
        lb3.call(Lists.newArrayList(1));
    }
    
    @Test
    public void firstHasClient() {
        FallbackLoadBalancer<Integer> lb = new FallbackLoadBalancer<Integer>(Lists.<LoadBalancer<Integer>>newArrayList(lb3, lb1, lb2));
        int value = lb.next();
        Assert.assertEquals(1, value);
    }
    
    @Test
    public void middleHasClient() {
        FallbackLoadBalancer<Integer> lb = new FallbackLoadBalancer<Integer>(Lists.<LoadBalancer<Integer>>newArrayList(lb1, lb3, lb2));
        int value = lb.next();
        Assert.assertEquals(1, value);
    }
    
    @Test
    public void lastHasClient() {
        FallbackLoadBalancer<Integer> lb = new FallbackLoadBalancer<Integer>(Lists.<LoadBalancer<Integer>>newArrayList(lb1, lb2, lb3));
        int value = lb.next();
        Assert.assertEquals(1, value);
    }
    
    @Test(expected=NoSuchElementException.class)
    public void noneHaveClient() {
        FallbackLoadBalancer<Integer> lb = new FallbackLoadBalancer<Integer>(Lists.<LoadBalancer<Integer>>newArrayList(lb1, lb2, lb2));
        int value = lb.next();
        Assert.assertEquals(1, value);
    }
    
    @Test(expected=NoSuchElementException.class)
    public void noLbs() {
        FallbackLoadBalancer<Integer> lb = new FallbackLoadBalancer(Lists.newArrayList());
        int value = lb.next();
        Assert.assertEquals(1, value);
    }
}
