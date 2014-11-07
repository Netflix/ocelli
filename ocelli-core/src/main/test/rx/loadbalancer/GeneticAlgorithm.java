package rx.loadbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.loadbalancer.client.Behaviors;
import rx.loadbalancer.client.Connects;
import rx.loadbalancer.client.ResponseObserver;
import rx.loadbalancer.client.TestClient;
import rx.loadbalancer.client.TestClientFactory;
import rx.loadbalancer.client.TestHost;
import rx.loadbalancer.client.TrackingOperation;
import rx.loadbalancer.loadbalancer.DefaultLoadBalancer;
import rx.loadbalancer.metrics.ClientMetrics;
import rx.loadbalancer.metrics.SimpleClientMetricsFactory;
import rx.loadbalancer.selectors.RoundRobinSelectionStrategy;

public class GeneticAlgorithm {
//    private static final Logger LOG = LoggerFactory.getLogger(GeneticAlgorithm.class);
//        
//    private static List<TestHost> hosts = new ArrayList<TestHost>();
//    
//    private DefaultLoadBalancer<TestHost, TestClient, ClientMetrics> selector;
//    
//    @BeforeClass
//    public static void setup() {
//        hosts = new ArrayList<TestHost>();
//        hosts.add(TestHost.create("dead-2", Connects.failure(10, TimeUnit.MILLISECONDS), Behaviors.immediate()));
//        hosts.add(TestHost.create("bad-0" , Connects.delay  (10, TimeUnit.MILLISECONDS), Behaviors.failure(10, TimeUnit.MILLISECONDS)));
//        hosts.add(TestHost.create("good-1", Connects.delay  (10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS)));
//    }
//    
//    @Before
//    public void beforeTest() {
//        this.selector = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
//                .withHostSource(Observable
//                    .from(hosts)
//                    .map(HostEvent.<TestHost>toAdd()))
//                .withConnector(new TestClientFactory())
//                .withClientTrackerFactory(new SimpleClientMetricsFactory<TestHost>())
//                .build();
//        
//        this.selector.initialize();
//    }
//    
//    @After
//    public void afterTest() {
//        if (this.selector != null) {
//            this.selector.shutdown();
//        }
//    }
//    
//    @Test
//    public void test() throws Throwable {
//        final TrackingOperation op = new TrackingOperation("response");
//        final ResponseObserver response = new ResponseObserver();
//        
//        selector
//            .select()
//            .concatMap(op)
//            .retry(2)
//            .subscribe(response);
//        
//        LOG.info(op.getServers().toString());
//        LOG.info("Response: " + response.get());
//    }
}
