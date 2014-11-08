package netflix.ocelli;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.HostEvent.EventType;
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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

public class LoadBalancerTest {
    private static final Logger LOG = LoggerFactory.getLogger(LoadBalancerTest.class);
    
    private static final TestHost  s1  = TestHost.create("1",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s2  = TestHost.create("2",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s3  = TestHost.create("3",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s4  = TestHost.create("4",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s5  = TestHost.create("5",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s6  = TestHost.create("6",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s7  = TestHost.create("7",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s8  = TestHost.create("8",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s9  = TestHost.create("9",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestHost  s10 = TestHost.create("10", Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));

    private static List<TestHost> servers;

    private DefaultLoadBalancer<TestHost, TestClient, ClientMetrics> selector;

    @Rule
    public TestName name = new TestName();
    
    @BeforeClass
    public static void setup() {
        servers = new ArrayList<TestHost>();
        servers.add(s1);
        servers.add(s2);
        servers.add(s3);
        servers.add(s4);
        servers.add(s5);
        servers.add(s6);
        servers.add(s7);
        servers.add(s8);
        servers.add(s9);
        servers.add(s10);
    }
    
    @Before 
    public void before() throws InterruptedException {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                    .from(servers)
                    .map(HostEvent.<TestHost>toEvent(EventType.ADD)))
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.selector.initialize();
        TimeUnit.SECONDS.sleep(1);
        
        LOG.info(">>>>>>>>>>>>>>>> " + name.getMethodName() + " <<<<<<<<<<<<<<<<");
    }
    
    @After
    public void after() {
        if (this.selector != null)
            this.selector.shutdown();
    }
    
    @Test
    public void testManualOperation() throws Throwable {
        final TrackingOperation op = new TrackingOperation("response");
        final ResponseObserver response = new ResponseObserver();

        selector
            .choose()
            .flatMap(op)
            .retry(2)
            .subscribe(response);
        
        response.await(10, TimeUnit.SECONDS);
        LOG.info("Response : " + response.get());
        LOG.info(op.getServers().toString());
        
        List<TestHost> expected = new ArrayList<TestHost>();
        expected.add(s2);
        
        Assert.assertEquals(expected, op.getServers());
    }
    
//    @Test
//    public void testAutoInvoker() throws InterruptedException {
//        final CountDownLatch latch = new CountDownLatch(1);
//        final TrackingOperation op = new TrackingOperation(2, "response");
//        
//        Subscription sub = loadBalancer
//            .acquire(SelectionStrategy.next())
//            .flatMap(op)
//            .retry(2)
//            .subscribe(new Action1<String>() {
//                @Override
//                public void call(String t1) {
//                    latch.countDown();
//                    LOG.info("Response: " + t1);
//                }
//            });
//        
//        latch.await();
//        sub.unsubscribe();
//        
//        LOG.info(op.getServers().toString());
//        Assert.assertEquals(Lists.newArrayList(s4,  s7), op.getServers());
//    }
}
