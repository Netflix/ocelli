package netflix.ocelli.loadbalancer.weighting;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.Assert;
import netflix.ocelli.loadbalancer.RandomWeightedLoadBalancer;
import netflix.ocelli.retry.RetryFailedTestRule;
import netflix.ocelli.retry.RetryFailedTestRule.Retry;

import org.junit.Rule;
import org.junit.Test;

import rx.subjects.BehaviorSubject;

import com.google.common.collect.Lists;

public class LinearWeightingStrategyTest extends BaseWeightingStrategyTest {
                
    @Rule
    public RetryFailedTestRule retryRule = new RetryFailedTestRule();
    
    @Test(expected=NoSuchElementException.class)
    public void testEmptyClients() throws Throwable {
        BehaviorSubject<List<IntClientAndMetrics>> subject = BehaviorSubject.create();
        RandomWeightedLoadBalancer<IntClientAndMetrics> selector = RandomWeightedLoadBalancer.create( 
                subject,
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
        
        List<IntClientAndMetrics> clients = create();
        subject.onNext(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.size(), 1000), 100));
        Assert.assertEquals(Lists.newArrayList(), counts);
    }
    
    @Test
    @Retry(5)
    public void testOneClient() throws Throwable {
        BehaviorSubject<List<IntClientAndMetrics>> subject = BehaviorSubject.create();
        RandomWeightedLoadBalancer<IntClientAndMetrics> selector = RandomWeightedLoadBalancer.create( 
                subject,
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
        
        List<IntClientAndMetrics> clients = create(10);
        subject.onNext(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.size(), 1000), 100));
        Assert.assertEquals(Lists.newArrayList(1000), counts);
    }
    
    @Test
    @Retry(5)
    public void testEqualsWeights() throws Throwable {
        BehaviorSubject<List<IntClientAndMetrics>> subject = BehaviorSubject.create();
        RandomWeightedLoadBalancer<IntClientAndMetrics> selector = RandomWeightedLoadBalancer.create( 
                subject,
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
        
        List<IntClientAndMetrics> clients = create(1,1,1,1);
        subject.onNext(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.size(), 4000), 100));
        Assert.assertEquals(Lists.newArrayList(1000, 1000, 1000, 1000), counts);
    }
    
    @Test
    @Retry(5)
    public void testDifferentWeights() throws Throwable {
        BehaviorSubject<List<IntClientAndMetrics>> subject = BehaviorSubject.create();
        RandomWeightedLoadBalancer<IntClientAndMetrics> selector = RandomWeightedLoadBalancer.create( 
                subject,
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
        
        List<IntClientAndMetrics> clients = create(1,2,3,4);
        subject.onNext(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.size(), 4000), 100));
        Assert.assertEquals(Lists.newArrayList(400, 800, 1200, 1600), counts);
    }
}
