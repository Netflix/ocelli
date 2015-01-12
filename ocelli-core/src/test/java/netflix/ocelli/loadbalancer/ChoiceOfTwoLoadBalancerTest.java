package netflix.ocelli.loadbalancer;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicIntegerArray;

import junit.framework.Assert;
import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import rx.functions.Func2;

public class ChoiceOfTwoLoadBalancerTest {
    private ChoiceOfTwoLoadBalancer<Integer> selector;
    
//    @Rule
//    public TestName name = new TestName();
//        
//    @Before
//    public void before() {
//        selector = new ChoiceOfTwoSelector<Integer>(new Func2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer t1, Integer t2) {
//                return (t1 > t2) ? t1 : t2;
//            }
//        });
//    }
//    
//    @Test(expected=NoSuchElementException.class)
//    public void testEmpty() {
//        Integer[] clients = new Integer[0];
//        selector.setClients(clients);
//        
//        selector.toBlocking().single();
//    }
//    
//    @Test
//    public void testOne() {
//        Integer[] clients = new Integer[]{0};
//        selector.setClients(clients);
//        
//        for (int i = 0; i < 100; i++) {
//            Assert.assertEquals(0, (int)selector.toBlocking().single());
//        }
//    }
//    
//    @Test
//    public void testTwo() {
//        Integer[] clients = new Integer[]{0,1};
//        selector.setClients(clients);
//        
//        AtomicIntegerArray counts = new AtomicIntegerArray(2);
//        
//        for (int i = 0; i < 100; i++) {
//            counts.incrementAndGet(selector.toBlocking().single());
//        }
//        Assert.assertEquals(counts.get(0), 0);
//        Assert.assertEquals(counts.get(1), 100);
//    }
//    
//    @Test
//    public void testMany() {
//        Integer[] clients = new Integer[]{0,1,2,3,4,5,6,7,8,9};
//        selector.setClients(clients);
//        
//        AtomicIntegerArray counts = new AtomicIntegerArray(clients.length);
//        
//        for (int i = 0; i < 100000; i++) {
//            counts.incrementAndGet(selector.toBlocking().single());
//        }
//        Double[] pct = new Double[clients.length];
//        for (int i = 0; i < clients.length; i++) {
//            pct[i] = counts.get(i)/100000.0;
//        }
//        
//        for (int i = 1; i < counts.length(); i++) {
//            Assert.assertTrue(counts.get(i) > counts.get(i-1));
//        }
//    }
}
