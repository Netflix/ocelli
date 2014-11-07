package netflix.ocelli;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.ResponseObserver;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TestClientFactory;
import netflix.ocelli.client.TestHost;
import netflix.ocelli.client.TrackingOperation;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import netflix.ocelli.metrics.ClientMetrics;
import netflix.ocelli.metrics.SimpleClientMetricsFactory;
import netflix.ocelli.selectors.RoundRobinSelectionStrategy;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

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
