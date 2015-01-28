package netflix.ocelli.selectors;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.Assert;
import netflix.ocelli.SelectionStrategy;
import netflix.ocelli.retry.RetryFailedTestRule;
import netflix.ocelli.retry.RetryFailedTestRule.Retry;
import netflix.ocelli.selectors.RandomWeightedSelector;
import netflix.ocelli.selectors.weighting.LinearWeightingStrategy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class LinearWeightingStrategyTest extends BaseWeightingStrategyTest {
    
    SelectionStrategy<IntClientAndMetrics> selector;
                
    @Rule
    public RetryFailedTestRule retryRule = new RetryFailedTestRule();
    
    @Before 
    public void before() {
        selector = new RandomWeightedSelector<IntClientAndMetrics>(
                new LinearWeightingStrategy<IntClientAndMetrics>(IntClientAndMetrics.BY_METRIC));
    }
    
    @Test(expected=NoSuchElementException.class)
    public void testEmptyClients() throws Throwable {
        IntClientAndMetrics[] clients = create();
        selector.setClients(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.length, 1000), 100));
        Assert.assertEquals(Lists.newArrayList(), counts);
    }
    
    @Test
    @Retry(5)
    public void testOneClient() throws Throwable {
        IntClientAndMetrics[] clients = create(10);
        selector.setClients(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.length, 1000), 100));
        Assert.assertEquals(Lists.newArrayList(1000), counts);
    }
    
    @Test
    @Retry(5)
    public void testEqualsWeights() throws Throwable {
        IntClientAndMetrics[] clients = create(1,1,1,1);
        selector.setClients(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.length, 4000), 100));
        Assert.assertEquals(Lists.newArrayList(1000, 1000, 1000, 1000), counts);
    }
    
    @Test
    @Retry(5)
    public void testDifferentWeights() throws Throwable {
        IntClientAndMetrics[] clients = create(1,2,3,4);
        selector.setClients(clients);
        
        List<Integer> counts = Arrays.<Integer>asList(roundToNearest(simulate(selector, clients.length, 4000), 100));
        Assert.assertEquals(Lists.newArrayList(400, 800, 1200, 1600), counts);
    }
}
