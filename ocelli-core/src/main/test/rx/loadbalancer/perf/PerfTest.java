package rx.loadbalancer.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.algorithm.LowestLatencyScoreStrategy;
import rx.loadbalancer.client.Behaviors;
import rx.loadbalancer.client.Connects;
import rx.loadbalancer.client.TestClient;
import rx.loadbalancer.client.TestClientFactory;
import rx.loadbalancer.client.TestHost;
import rx.loadbalancer.client.TrackingOperation;
import rx.loadbalancer.loadbalancer.DefaultLoadBalancer;
import rx.loadbalancer.metrics.ClientMetrics;
import rx.loadbalancer.metrics.SimpleClientMetricsFactory;
import rx.loadbalancer.selector.DefaultLoadBalancerTest;
import rx.schedulers.Schedulers;

public class PerfTest {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancerTest.class);
    
    private static final int NUM_HOSTS = 10;
    private static Observable<HostEvent<TestHost>> source;
    private static final Random RANDOM = new Random();
    
    private DefaultLoadBalancer<TestHost, TestClient, ClientMetrics> selector;
    
    @BeforeClass
    public static void setup() {
        List<TestHost> hosts = new ArrayList<TestHost>();
        for (int i = 0; i < NUM_HOSTS-1; i++) {
//            hosts.add(TestHost.create("host-"+i, Connects.immediate(), Behaviors.delay(100 + (int)(100 * RANDOM.nextDouble()), TimeUnit.MILLISECONDS)));
            hosts.add(TestHost.create("host-"+i, Connects.immediate(), Behaviors.proportionalToLoad(100, 10, TimeUnit.MILLISECONDS)));
        }
        
        hosts.add(TestHost.create("degrading", Connects.immediate(), Behaviors.degradation(100, 50, TimeUnit.MILLISECONDS)));
        
        source = Observable
            .from(hosts)
            .map(HostEvent.<TestHost>toAdd());
    }
    
    @After
    public void afterTest() {
        if (this.selector != null) {
            this.selector.shutdown();
        }
    }
    
    @Test
    public void perf() throws InterruptedException {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(source)
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
                .withWeightingStrategy(new LowestLatencyScoreStrategy<TestHost, TestClient, ClientMetrics>())
                .build();
        
        this.selector.initialize();
//        this.selector.prime(10).toBlocking().last();

        Observable.range(1, 10)
            .subscribe(new Action1<Integer>() {
                @Override
                public void call(final Integer id) {
                    Observable.interval(100, TimeUnit.MILLISECONDS, Schedulers.newThread())
                        .subscribe(new Action1<Long>() {
                           @Override
                            public void call(final Long counter) {
                               selector.choose()
                                   .concatMap(new TrackingOperation(counter + ""))
                                   .retry()
                                   .subscribe(new Action1<String>() {
                                       @Override
                                       public void call(String t1) {
                                           LOG.info("{} - {} - {}", id, counter, t1);
                                       }
                                   });
                            }
                        });
                }
            });
        
        TimeUnit.SECONDS.sleep(100);
    }
}
