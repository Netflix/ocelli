package netflix.ocelli;

import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.ResponseObserver;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TrackingOperation;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LoadBalancerTest {
    private static final Logger LOG = LoggerFactory.getLogger(LoadBalancerTest.class);
    
    private static final TestClient  s1  = TestClient.create("1",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s2  = TestClient.create("2",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s3  = TestClient.create("3",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s4  = TestClient.create("4",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s5  = TestClient.create("5",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s6  = TestClient.create("6",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s7  = TestClient.create("7",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s8  = TestClient.create("8",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s9  = TestClient.create("9",  Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));
    private static final TestClient  s10 = TestClient.create("10", Connects.delay(10, TimeUnit.MILLISECONDS), Behaviors.delay(10, TimeUnit.MILLISECONDS));

    private static List<TestClient> servers;

    private LoadBalancer<TestClient> selector;

    @Rule
    public TestName name = new TestName();
    
    @BeforeClass
    public static void setup() {
        servers = new ArrayList<TestClient>();
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
        this.selector = LoadBalancers.fromHostSource(Observable.from(servers)
                                                               .map(MembershipEvent.<TestClient>toEvent(EventType.ADD)));

        LOG.info(">>>>>>>>>>>>>>>> " + name.getMethodName() + " <<<<<<<<<<<<<<<<");
    }
    
    @After
    public void after() {
        if (this.selector != null)
            this.selector.shutdown();
    }
    
    @Test
    @Ignore
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
        
        List<TestClient> expected = new ArrayList<TestClient>();
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
