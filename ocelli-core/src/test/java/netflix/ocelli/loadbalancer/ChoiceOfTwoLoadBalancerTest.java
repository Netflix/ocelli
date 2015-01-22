package netflix.ocelli.loadbalancer;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicIntegerArray;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import rx.functions.Func2;
import rx.subjects.PublishSubject;

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
        PublishSubject<Integer[]> source = PublishSubject.create();
        ChoiceOfTwoLoadBalancer<Integer> lb = ChoiceOfTwoLoadBalancer.create(source, COMPARATOR);
        
        source.onNext(new Integer[0]);
        
        lb.toBlocking().single();
    }
    
    @Test
    public void testOne() {
        PublishSubject<Integer[]> source = PublishSubject.create();
        ChoiceOfTwoLoadBalancer<Integer> lb = ChoiceOfTwoLoadBalancer.create(source, COMPARATOR);
        
        source.onNext(new Integer[]{0});
        
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(0, (int)lb.toBlocking().single());
        }
    }
    
    @Test
    public void testTwo() {
        PublishSubject<Integer[]> source = PublishSubject.create();
        ChoiceOfTwoLoadBalancer<Integer> lb = ChoiceOfTwoLoadBalancer.create(source, COMPARATOR);
        
        source.onNext(new Integer[]{0,1});
        
        AtomicIntegerArray counts = new AtomicIntegerArray(2);
        
        for (int i = 0; i < 100; i++) {
            counts.incrementAndGet(lb.toBlocking().single());
        }
        Assert.assertEquals(counts.get(0), 0);
        Assert.assertEquals(counts.get(1), 100);
    }
    
    @Test
    public void testMany() {
        PublishSubject<Integer[]> source = PublishSubject.create();
        ChoiceOfTwoLoadBalancer<Integer> lb = ChoiceOfTwoLoadBalancer.create(source, COMPARATOR);
        
        source.onNext(new Integer[]{0,1,2,3,4,5,6,7,8,9});
        
        AtomicIntegerArray counts = new AtomicIntegerArray(10);
        
        for (int i = 0; i < 100000; i++) {
            counts.incrementAndGet(lb.toBlocking().single());
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
