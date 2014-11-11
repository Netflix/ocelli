package netflix.ocelli.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import netflix.ocelli.HostEvent;
import netflix.ocelli.HostEvent.EventType;
import netflix.ocelli.algorithm.LowestLatencyScoreStrategy;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TestClientFactory;
import netflix.ocelli.client.TestHost;
import netflix.ocelli.client.TrackingOperation;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import netflix.ocelli.metrics.CoreClientMetricsFactory;
import netflix.ocelli.util.Functions;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

public class PerfTest {
    private static final Logger LOG = LoggerFactory.getLogger(PerfTest.class);
    
    private static final int NUM_HOSTS = 1000;
    private static Observable<HostEvent<TestHost>> source;
    private static final Random RANDOM = new Random();
    
    private DefaultLoadBalancer<TestHost, TestClient> selector;
    
    @BeforeClass
    public static void setup() {
        List<TestHost> hosts = new ArrayList<TestHost>();
        for (int i = 0; i < NUM_HOSTS-1; i++) {
//            hosts.add(TestHost.create("host-"+i, Connects.immediate(), Behaviors.delay(100 + (int)(100 * RANDOM.nextDouble()), TimeUnit.MILLISECONDS)));
//          hosts.add(TestHost.create("host-"+i, Connects.immediate(), Behaviors.proportionalToLoad(100, 10, TimeUnit.MILLISECONDS)));
            hosts.add(TestHost.create("host-"+i, Connects.immediate(), Behaviors.immediate()));
        }
        
//        hosts.add(TestHost.create("degrading", Connects.immediate(), Behaviors.degradation(100, 50, TimeUnit.MILLISECONDS)));
        
        source = Observable
            .from(hosts)
            .map(HostEvent.<TestHost>toEvent(EventType.ADD));
    }
    
    @After
    public void afterTest() {
        if (this.selector != null) {
            this.selector.shutdown();
        }
    }
    
    @Test
    public void perf() throws InterruptedException {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withHostSource(source)
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new CoreClientMetricsFactory<TestHost>())
                .withConnectedHostCountStrategy(Functions.sqrt())
//                .withWeightingStrategy(new LowestLatencyScoreStrategy<TestHost, TestClient, ClientMetrics>())
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
    
    @Test
    public void perf2() throws InterruptedException {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withHostSource(source)
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new CoreClientMetricsFactory<TestHost>())
                .withWeightingStrategy(new LowestLatencyScoreStrategy<TestHost, TestClient>())
                .withConnectedHostCountStrategy(Functions.sqrt())
                .build();
        
        this.selector.initialize();

        final AtomicLong messageCount = new AtomicLong(0);
        
        Observable.range(1, 400)
            .subscribe(new Action1<Integer>() {
                @Override
                public void call(final Integer id) {
                    Observable.interval(10, TimeUnit.MILLISECONDS, Schedulers.newThread())
                        .subscribe(new Action1<Long>() {
                           @Override
                            public void call(final Long counter) {
                               selector.choose()
                                   .concatMap(new TrackingOperation(counter + ""))
                                   .retry()
                                   .subscribe(new Action1<String>() {
                                       @Override
                                       public void call(String t1) {
                                           messageCount.incrementAndGet();
                                           
//                                           LOG.info("{} - {} - {}", id, counter, t1);
                                       }
                                   });
                            }
                        });
                }
            });
        
        Observable.interval(1, TimeUnit.SECONDS).subscribe(new Action1<Long>() {
            private long previous = 0;
            @Override
            public void call(Long t1) {
                long current = messageCount.get();
                LOG.info("Rate : " + (current - previous) + " Host count: " + selector.listActiveClients().count().toBlocking().first());
                previous = current;
            }
        });
        TimeUnit.SECONDS.sleep(100);
    }
}
