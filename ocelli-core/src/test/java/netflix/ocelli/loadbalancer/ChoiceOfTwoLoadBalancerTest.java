package netflix.ocelli.loadbalancer;

import com.google.common.collect.Lists;
import netflix.ocelli.LoadBalancer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import rx.subjects.BehaviorSubject;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class ChoiceOfTwoLoadBalancerTest {
    @Rule
    public TestName name = new TestName();
        
    private static final Comparator<Integer> COMPARATOR = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1 - o2;
        }
    };
    
    @Test(expected=NoSuchElementException.class)
    public void testEmpty() {
        BehaviorSubject<List<Integer>> source = BehaviorSubject.create();
        
        LoadBalancer<Integer> lb = LoadBalancer.fromSnapshotSource(source).build(ChoiceOfTwoLoadBalancer.create(COMPARATOR));
        
        source.onNext(Lists.<Integer>newArrayList());
        
        lb.next();
    }
    
    @Test
    public void testOne() {
        BehaviorSubject<List<Integer>> source = BehaviorSubject.create();
        LoadBalancer<Integer> lb = LoadBalancer.fromSnapshotSource(source).build(ChoiceOfTwoLoadBalancer.create(COMPARATOR));
        
        source.onNext(Lists.newArrayList(0));

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(0, (int)lb.next());
        }
    }
    
    @Test
    public void testTwo() {
        BehaviorSubject<List<Integer>> source = BehaviorSubject.create();
        LoadBalancer<Integer> lb = LoadBalancer.fromSnapshotSource(source).build(ChoiceOfTwoLoadBalancer.create(COMPARATOR));
        
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
        LoadBalancer<Integer> lb = LoadBalancer.fromSnapshotSource(source).build(ChoiceOfTwoLoadBalancer.create(COMPARATOR));
        
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
