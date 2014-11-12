package netflix.ocelli.algorithm;

import java.util.List;

import junit.framework.Assert;
import netflix.ocelli.ClientAndMetrics;
import netflix.ocelli.retry.RetryFailedTestRule;
import netflix.ocelli.retry.RetryFailedTestRule.Retry;
import netflix.ocelli.selectors.ClientsAndWeights;
import netflix.ocelli.selectors.RandomWeightSelector;
import netflix.ocelli.selectors.RoundRobinWeightSelector;
import netflix.ocelli.selectors.WeightSelector;
import netflix.ocelli.selectors.WeightedSelectionStrategy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

import rx.functions.Action1;
import rx.functions.Functions;

public class LinearWeightingStrategyTest {
    
    public static class IntClientAndMetrics implements ClientAndMetrics<Integer, Integer> {
        private Integer client;
        private Integer metrics;
        
        public IntClientAndMetrics(int client, int metrics) {
            this.client = client;
            this.metrics = metrics;
        }
        
        @Override
        public Integer getClient() {
            return client;
        }

        @Override
        public Integer getMetrics() {
            return metrics;
        }
    }
    
    LinearWeightingStrategy<Integer, Integer> strategy;

    @Rule
    public RetryFailedTestRule retryRule = new RetryFailedTestRule();
    
    @Before 
    public void before() {
        strategy = new LinearWeightingStrategy<Integer, Integer>(Functions.<Integer>identity());
    }
    
    @Test
    public void testEmptyClients() {
        ClientsAndWeights<Integer> result = strategy.call(create());
        
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
        ClientsAndWeights<Integer> result = strategy.call(create(10));
        
        Assert.assertEquals(Lists.newArrayList(10), getWeights(result));
        
        List<Integer> counts;
        counts = roundToNearest(select(result, new RandomWeightSelector(), 1000), 100);
        Assert.assertEquals(Lists.newArrayList(1000), counts);
        
        counts = select(result, new RoundRobinWeightSelector(), 1000);
        Assert.assertEquals(Lists.newArrayList(1000), counts);

    }
    
    @Test
    @Retry(5)
    public void testEqualsWeights() {
        ClientsAndWeights<Integer> result = strategy.call(create(1,1,1,1));
        
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
        ClientsAndWeights<Integer> result = strategy.call(create(1,2,3,4));
        
        Assert.assertEquals(Lists.newArrayList(1,3,6,10), getWeights(result));
        
        List<Integer> counts;
        counts = roundToNearest(select(result, new RandomWeightSelector(), 4000), 100);
        Assert.assertEquals(Lists.newArrayList(400, 800, 1200, 1600), counts);
        
        counts = select(result, new RoundRobinWeightSelector(), 4000);
        Assert.assertEquals(Lists.newArrayList(400, 800, 1200, 1600), counts);

    }
    
    private List<ClientAndMetrics<Integer, Integer>> create(Integer... weights) {
        List<ClientAndMetrics<Integer, Integer>> cam = Lists.newArrayList();
        int counter = 0;
        for (int weight : weights) {
            cam.add(new IntClientAndMetrics(counter++, weight));
        }
        return cam;
    }
    
    private List<Integer> getWeights(ClientsAndWeights<Integer> caw) {
        List<Integer> weights = Lists.newArrayList();
        for (int weight : caw.getWeights()) {
            weights.add(weight);
        }
        return weights;
    }
    
    private List<Integer> select(ClientsAndWeights<Integer> caw, WeightSelector selector, int count) {
        WeightedSelectionStrategy<Integer> select = new WeightedSelectionStrategy<Integer>(selector);
        final List<Integer> counts = Lists.newArrayList();
        for (int i = 0; i < caw.getClients().size(); i++) {
            counts.add(0);
        }
        for (int i = 0; i < count; i++) {
            select.call(caw).subscribe(new Action1<Integer>() {
                @Override
                public void call(Integer t1) {
                    counts.set(t1, counts.get(t1) + 1);
                }
            });
        }
        return counts;
    }
    
    private List<Integer> roundToNearest(List<Integer> counts, int amount) {
        int middle = amount / 2;
        for (int i = 0; i < counts.size(); i++) {
            counts.set(i, amount * ((counts.get(i) + middle) / amount));
        }
        return counts;
    }
}
