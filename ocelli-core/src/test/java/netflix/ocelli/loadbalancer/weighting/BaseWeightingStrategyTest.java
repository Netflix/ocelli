package netflix.ocelli.loadbalancer.weighting;

import java.util.ArrayList;
import java.util.List;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.loadbalancer.weighting.ClientsAndWeights;

import org.junit.Ignore;

import rx.Observable;
import rx.exceptions.OnErrorNotImplementedException;
import rx.functions.Action1;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

@Ignore
public class BaseWeightingStrategyTest {

    /**
     * Creates a list of clients
     * 
     * @param weights
     * @return
     */
    static List<IntClientAndMetrics> create(Integer... weights) {
        List<IntClientAndMetrics> cam = new ArrayList<IntClientAndMetrics>(weights.length);
        int counter = 0;
        for (int i = 0; i < weights.length; i++) {
            cam.add(new IntClientAndMetrics(counter++, weights[i]));
        }
        return cam;
    }
    
    /**
     * Get an array of weights with indexes matching the list of clients.
     * @param caw
     * @return
     */
    static int[] getWeights(ClientsAndWeights<IntClientAndMetrics> caw) {
        int[] weights = new int[caw.size()];
        for (int i = 0; i < caw.size(); i++) {
            weights[i] = caw.getWeight(i);
        }
        return weights;
    }
    
    /**
     * Run a simulation of 'count' selects and update the clients
     * @param strategy
     * @param N
     * @param count
     * @return
     * @throws Throwable 
     */
    static Integer[] simulate(LoadBalancer<IntClientAndMetrics> select, int N, int count) throws Throwable {
        // Set up array of counts
        final Integer[] counts = new Integer[N];
        for (int i = 0; i < N; i++) {
            counts[i] = 0;
        }

        // Run simulation
        for (int i = 0; i < count; i++) {
            try {
                select.toObservable().subscribe(new Action1<IntClientAndMetrics>() {
                    @Override
                    public void call(IntClientAndMetrics t1) {
                        counts[t1.getClient()] = counts[t1.getClient()] + 1;
                    }
                });
            }
            catch (OnErrorNotImplementedException e) {
                throw e.getCause();
            }
        }
        return counts;
    }
    
    static Integer[] roundToNearest(Integer[] counts, int amount) {
        int middle = amount / 2;
        for (int i = 0; i < counts.length; i++) {
            counts[i] = amount * ((counts[i] + middle) / amount);
        }
        return counts;
    }
    
    static String printClients(IntClientAndMetrics[] clients) {
        return Joiner.on(", ").join(Collections2.transform(Lists.newArrayList(clients), new Function<IntClientAndMetrics, Integer>() {
            @Override
            public Integer apply(IntClientAndMetrics arg0) {
                return arg0.getClient();
            }
        }));
    }
    
    static String printMetrics(IntClientAndMetrics[] clients) {
        return Joiner.on(", ").join(Collections2.transform(Lists.newArrayList(clients), new Function<IntClientAndMetrics, Integer>() {
            @Override
            public Integer apply(IntClientAndMetrics arg0) {
                return arg0.getMetrics();
            }
        }));
    }
}
