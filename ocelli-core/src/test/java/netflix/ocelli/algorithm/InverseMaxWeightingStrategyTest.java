package netflix.ocelli.algorithm;

import java.util.List;

import junit.framework.Assert;
import netflix.ocelli.retry.RetryFailedTestRule;
import netflix.ocelli.retry.RetryFailedTestRule.Retry;
import netflix.ocelli.selectors.ClientsAndWeights;
import netflix.ocelli.selectors.RandomWeightSelector;
import netflix.ocelli.selectors.RoundRobinWeightSelector;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import rx.functions.Func1;

import com.google.common.collect.Lists;

public class InverseMaxWeightingStrategyTest extends BaseWeightingStrategyTest {
    InverseMaxWeightingStrategy<IntClientAndMetrics> strategy;

    @Rule
    public RetryFailedTestRule retryRule = new RetryFailedTestRule();
    
    @Before 
    public void before() {
        strategy = new InverseMaxWeightingStrategy<IntClientAndMetrics>(new Func1<IntClientAndMetrics, Integer>() {
            @Override
            public Integer call(IntClientAndMetrics t1) {
                return t1.getMetrics();
            }            
        });
    }
    
    @Test
    public void testEmptyClients() {
        ClientsAndWeights<IntClientAndMetrics> result = strategy.call(create());
        
        Assert.assertEquals(Lists.newArrayList(), getWeights(result));
        
        List<Integer> counts;
        counts = roundToNearest(select(result, new RandomWeightSelector(), 1000), 100);
        Assert.assertEquals(Lists.newArrayList(), counts);
        
        counts = select(result, new RoundRobinWeightSelector(), 1000);
        Assert.assertEquals(Lists.newArrayList(), counts);
    }
    
    @Test
    @Retry(5)
    public void testOneClient() {
        ClientsAndWeights<IntClientAndMetrics> result = strategy.call(create(10));
        
        Assert.assertEquals(Lists.newArrayList(1), getWeights(result));
        
        List<Integer> counts;
        counts = roundToNearest(select(result, new RandomWeightSelector(), 1000), 100);
        Assert.assertEquals(Lists.newArrayList(1000), counts);
        
        counts = select(result, new RoundRobinWeightSelector(), 1000);
        Assert.assertEquals(Lists.newArrayList(1000), counts);

    }
    
    @Test
    @Retry(5)
    public void testEqualsWeights() {
        ClientsAndWeights<IntClientAndMetrics> result = strategy.call(create(1,1,1,1));
        
        Assert.assertEquals(Lists.newArrayList(1,2,3,4), getWeights(result));
        
        List<Integer> counts;
        counts = roundToNearest(select(result, new RandomWeightSelector(), 4000), 100);
        Assert.assertEquals(Lists.newArrayList(1000, 1000, 1000, 1000), counts);
        
        counts = select(result, new RoundRobinWeightSelector(), 4000);
        Assert.assertEquals(Lists.newArrayList(1000, 1000, 1000, 1000), counts);

    }
    
    @Test
    @Retry(5)
    public void testDifferentWeights() {
        ClientsAndWeights<IntClientAndMetrics> result = strategy.call(create(1,2,3,4));
        
        Assert.assertEquals(Lists.newArrayList(4,7,9,10), getWeights(result));
        
        List<Integer> counts;
        counts = roundToNearest(select(result, new RandomWeightSelector(), 4000), 100);
        Assert.assertEquals(Lists.newArrayList(1600, 1200, 800, 400), counts);
        
        counts = select(result, new RoundRobinWeightSelector(), 4000);
        Assert.assertEquals(Lists.newArrayList(1600, 1200, 800, 400), counts);

    }
    
}
