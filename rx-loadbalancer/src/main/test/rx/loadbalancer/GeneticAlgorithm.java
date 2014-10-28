package rx.loadbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observer;
import rx.loadbalancer.client.Behaviors;
import rx.loadbalancer.client.Connects;
import rx.loadbalancer.client.TestClient;
import rx.loadbalancer.client.TestHost;
import rx.loadbalancer.loadbalancer.RoundRobinLoadBalancer;
import rx.loadbalancer.metrics.ClientMetrics;
import rx.loadbalancer.metrics.SimpleClientMetricsFactory;
import rx.loadbalancer.operations.TrackingOperation;
import rx.loadbalancer.selector.DefaultClientSelector;

public class GeneticAlgorithm {
    private static final Logger LOG = LoggerFactory.getLogger(GeneticAlgorithm.class);
        
    private static List<TestHost> hosts = new ArrayList<TestHost>();
    
    private RoundRobinLoadBalancer<TestClient> loadBalancer;

    private DefaultClientSelector<TestHost, TestClient, ClientMetrics> selector;
    
    @BeforeClass
    public static void setup() {
        hosts = new ArrayList<TestHost>();
        hosts.add(TestHost.create("dead-2", Connects.failure(10, TimeUnit.MILLISECONDS,   new Exception("Bad Connection")), Behaviors.immediate()));
        hosts.add(TestHost.create("bad-0" , Connects.delay  (10, TimeUnit.MILLISECONDS),  Behaviors.failure(10, TimeUnit.MILLISECONDS, new Exception("Bad Host"))));
        hosts.add(TestHost.create("good-1", Connects.delay  (10, TimeUnit.MILLISECONDS),  Behaviors.delay(10, TimeUnit.MILLISECONDS)));
    }
    
    @Before
    public void beforeTest() {
        this.selector = DefaultClientSelector.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                    .from(hosts)
                    .map(HostEvent.<TestHost>toAdd()))
                .withConnector(new TestClientFactory())
                .withClientTrackerFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.selector.initialize();
        
        loadBalancer = new RoundRobinLoadBalancer<TestClient>(selector.aquire());
    }
    
    @After
    public void afterTest() {
        if (this.selector != null) {
            this.selector.shutdown();
        }
    }
    
    public static class ResponseHolder implements Observer<String> {
        private volatile Throwable t;
        private String response;
        private CountDownLatch latch = new CountDownLatch(1);
        
        @Override
        public void onCompleted() {
            latch.countDown();
        }

        @Override
        public void onError(Throwable e) {
            this.t = e;
            latch.countDown();
        }

        @Override
        public void onNext(String t) {
            this.response = t;
        }
        
        public String get() throws Throwable {
            latch.await();
            if (this.t != null)
                throw this.t;
            return response;
        }
    }
    
    @Test
    public void test() throws Throwable {
        final TrackingOperation op = new TrackingOperation("response");
        final ResponseHolder response = new ResponseHolder();
        
        loadBalancer
            .select()
            .concatMap(op)
            .retry(2)
            .subscribe(response);
        
        LOG.info(op.getServers().toString());
        LOG.info("Response: " + response.get());
    }
}
