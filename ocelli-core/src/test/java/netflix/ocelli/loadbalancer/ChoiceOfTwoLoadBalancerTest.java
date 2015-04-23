package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicIntegerArray;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import rx.functions.Func2;
import rx.subjects.BehaviorSubject;

import com.google.common.collect.Lists;

public class ChoiceOfTwoLoadBalancerTest {
    @Rule
    public TestName name = new TestName();
        
    private static Func2<Integer, Integer, Integer> COMPARATOR = new Func2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer t1, Integer t2) {
            return (t1 > t2) ? t1 : t2;
        }
    };
    
    @Test(expected=NoSuchElementException.class)
    public void testEmpty() {
        BehaviorSubject<List<Integer>> source = BehaviorSubject.create();
        ChoiceOfTwoLoadBalancer<Integer> lb = ChoiceOfTwoLoadBalancer.create(source, COMPARATOR);
        
        source.onNext(Lists.<Integer>newArrayList());
        
        lb.next();
    }
    
    @Test
    public void testOne() {
        BehaviorSubject<List<Integer>> source = BehaviorSubject.create();
        ChoiceOfTwoLoadBalancer<Integer> lb = ChoiceOfTwoLoadBalancer.create(source, COMPARATOR);
        
        source.onNext(Lists.newArrayList(0));

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(0, (int)lb.next());
        }
    }
    
    @Test
    public void testTwo() {
        BehaviorSubject<List<Integer>> source = BehaviorSubject.create();
        ChoiceOfTwoLoadBalancer<Integer> lb = ChoiceOfTwoLoadBalancer.create(source, COMPARATOR);
        
        source.onNext(Lists.newArrayList(0,1));
        
        AtomicIntegerArray counts = new AtomicIntegerArray(2);
        
        for (int i = 0; i < 100; i++) {
            counts.incrementAndGet(lb.next());
        }
        Assert.assertEquals(counts.get(0), 0);
        Assert.assertEquals(counts.get(1), 100);
    }
    
    @Test
    public void testMany() {
        BehaviorSubject<List<Integer>> source = BehaviorSubject.create();
        ChoiceOfTwoLoadBalancer<Integer> lb = ChoiceOfTwoLoadBalancer.create(source, COMPARATOR);
        
        source.onNext(Lists.newArrayList(0,1,2,3,4,5,6,7,8,9));
        
        AtomicIntegerArray counts = new AtomicIntegerArray(10);
        
        for (int i = 0; i < 100000; i++) {
            counts.incrementAndGet(lb.next());
        }
        Double[] pct = new Double[counts.length()];
        for (int i = 0; i < counts.length(); i++) {
            pct[i] = counts.get(i)/100000.0;
        }
        
        for (int i = 1; i < counts.length(); i++) {
            Assert.assertTrue(counts.get(i) > counts.get(i-1));
        }
    }

}
