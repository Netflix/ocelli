package netflix.ocelli.algorithm;

import java.util.List;

import netflix.ocelli.selectors.ClientsAndWeights;
import netflix.ocelli.selectors.WeightSelector;
import netflix.ocelli.selectors.WeightedSelectionStrategy;

import org.junit.Ignore;

import rx.functions.Action1;

import com.google.common.collect.Lists;

@Ignore
public class BaseWeightingStrategyTest {

    static List<IntClientAndMetrics> create(Integer... weights) {
        List<IntClientAndMetrics> cam = Lists.newArrayList();
        int counter = 0;
        for (int weight : weights) {
            cam.add(new IntClientAndMetrics(counter++, weight));
        }
        return cam;
    }
    
    static List<Integer> getWeights(ClientsAndWeights<IntClientAndMetrics> caw) {
        List<Integer> weights = Lists.newArrayList();
        for (int weight : caw.getWeights()) {
            weights.add(weight);
        }
        return weights;
    }
    
    static List<Integer> select(ClientsAndWeights<IntClientAndMetrics> caw, WeightSelector selector, int count) {
        WeightedSelectionStrategy<IntClientAndMetrics> select = new WeightedSelectionStrategy<IntClientAndMetrics>(selector);
        final List<Integer> counts = Lists.newArrayList();
        for (int i = 0; i < caw.getClients().size(); i++) {
            counts.add(0);
        }
        for (int i = 0; i < count; i++) {
            select.call(caw).subscribe(new Action1<IntClientAndMetrics>() {
                @Override
                public void call(IntClientAndMetrics t1) {
                    counts.set(t1.getClient(), counts.get(t1.getClient()) + 1);
                }
            });
        }
        return counts;
    }
    
    static List<Integer> roundToNearest(List<Integer> counts, int amount) {
        int middle = amount / 2;
        for (int i = 0; i < counts.size(); i++) {
            counts.set(i, amount * ((counts.get(i) + middle) / amount));
        }
        return counts;
    }
}
