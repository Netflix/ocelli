package netflix.ocelli.loadbalancer.weighting;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.Assert;
import netflix.ocelli.loadbalancer.RandomWeightedLoadBalancer;
import netflix.ocelli.loadbalancer.weighting.LinearWeightingStrategy;
import netflix.ocelli.retry.RetryFailedTestRule;
import netflix.ocelli.retry.RetryFailedTestRule.Retry;

import org.junit.Rule;
import org.junit.Test;

import rx.subjects.PublishSubject;

import com.google.common.collect.Lists;

public class LinearWeightingStrategyTest extends BaseWeightingStrategyTest {
                
    @Rule
    public RetryFailedTestRule retryRule = new RetryFailedTestRule();
    
    @Test(expected=NoSuchElementException.class)
    public void testEmptyClients() throws Throwable {
        PublishSubject<IntClientAndMetrics[]> subject = PublishSubject.create();
        RandomWeightedLoadBalancer<IntClientAndMetrics> selector = RandomWeightedLoadBalancer.create(subject, 
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
        
        IntClientAndMetrics[] clients = create();
        subject.onNext(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.length, 1000), 100));
        Assert.assertEquals(Lists.newArrayList(), counts);
    }
    
    @Test
    @Retry(5)
    public void testOneClient() throws Throwable {
        PublishSubject<IntClientAndMetrics[]> subject = PublishSubject.create();
        RandomWeightedLoadBalancer<IntClientAndMetrics> selector = RandomWeightedLoadBalancer.create(subject, 
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
        
        IntClientAndMetrics[] clients = create(10);
        subject.onNext(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.length, 1000), 100));
        Assert.assertEquals(Lists.newArrayList(1000), counts);
    }
    
    @Test
    @Retry(5)
    public void testEqualsWeights() throws Throwable {
        PublishSubject<IntClientAndMetrics[]> subject = PublishSubject.create();
        RandomWeightedLoadBalancer<IntClientAndMetrics> selector = RandomWeightedLoadBalancer.create(subject, 
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
        
        IntClientAndMetrics[] clients = create(1,1,1,1);
        subject.onNext(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.length, 4000), 100));
        Assert.assertEquals(Lists.newArrayList(1000, 1000, 1000, 1000), counts);
    }
    
    @Test
    @Retry(5)
    public void testDifferentWeights() throws Throwable {
        PublishSubject<IntClientAndMetrics[]> subject = PublishSubject.create();
        RandomWeightedLoadBalancer<IntClientAndMetrics> selector = RandomWeightedLoadBalancer.create(subject, 
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
        
        IntClientAndMetrics[] clients = create(1,2,3,4);
        subject.onNext(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.length, 4000), 100));
        Assert.assertEquals(Lists.newArrayList(400, 800, 1200, 1600), counts);
    }
}
