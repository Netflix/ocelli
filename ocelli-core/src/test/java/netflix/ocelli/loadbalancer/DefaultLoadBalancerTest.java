package netflix.ocelli.loadbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.LoadBalancerBuilder;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.Ocelli;
import netflix.ocelli.algorithm.LinearWeightingStrategy;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.ManualFailureDetector;
import netflix.ocelli.client.Operations;
import netflix.ocelli.client.ResponseObserver;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TestClientConnectorFactory;
import netflix.ocelli.client.TrackingOperation;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Functions;
import netflix.ocelli.functions.Retrys;
import netflix.ocelli.util.CountDownAction;
import netflix.ocelli.util.RxUtil;

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
import rx.subjects.PublishSubject;

public class DefaultLoadBalancerTest {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancerTest.class);
    
    private static final int NUM_HOSTS = 10;
    private static Observable<MembershipEvent<TestClient>> source;
    
    private LoadBalancerBuilder<TestClient> builder;
    private DefaultLoadBalancer<TestClient> lb;
    private PublishSubject<MembershipEvent<TestClient>> hostEvents = PublishSubject.create();
    private TestClientConnectorFactory clientConnector = new TestClientConnectorFactory();
    private ManualFailureDetector failureDetector = new ManualFailureDetector();
    
    @Rule
    public TestName testName = new TestName();
    
    @BeforeClass
    public static void setup() {
        List<TestClient> hosts = new ArrayList<TestClient>();
        for (int i = 0; i < NUM_HOSTS; i++) {
            hosts.add(TestClient.create("host-"+i, Connects.immediate(), Behaviors.immediate()));
        }
        
        source = Observable
            .from(hosts)
            .map(MembershipEvent.<TestClient>toEvent(EventType.ADD));
    }
    
    @Before 
    public void before() {
        builder = Ocelli.<TestClient>newDefaultLoadBalancerBuilder()
            .withName("Test-" + testName.getMethodName())
            .withMembershipSource(hostEvents)
            .withActiveClientCountStrategy(Functions.identity())
            .withQuaratineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
            .withFailureDetector(failureDetector)
            .withClientConnector(clientConnector)
            .withWeightingStrategy(new LinearWeightingStrategy<TestClient>(TestClient.byPendingRequestCount()));
    }
    
    @After
    public void afterTest() {
        if (this.lb != null) {
            this.lb.shutdown();
        }
    }
    
    @Test
    public void openConnectionImmediately() throws Throwable {
        TestClient client = TestClient.create("h1", Connects.immediate(), Behaviors.immediate());
        
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        this.lb.initialize();
        
        CountDownAction<TestClient> counter = new CountDownAction<TestClient>(1);
        clientConnector.get(client).stream().subscribe(counter);
        
        hostEvents.onNext(MembershipEvent.create(client, MembershipEvent.EventType.ADD));
        
        counter.await(1, TimeUnit.SECONDS);
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb.choose()
            .concatMap(operation)
            .retry()
            .subscribe(response);
        
        Assert.assertEquals("foo", response.get());
    }
    
    @Test
    public void removeClientFromSource() {
        TestClient client = TestClient.create("h1", Connects.immediate(), Behaviors.immediate());
        
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        this.lb.initialize();
        
        hostEvents.onNext(MembershipEvent.create(client, MembershipEvent.EventType.ADD));
        Assert.assertEquals(1, (int)this.lb.listActiveClients().count().toBlocking().first());
        
        hostEvents.onNext(MembershipEvent.create(client, MembershipEvent.EventType.REMOVE));
        Assert.assertEquals(0, (int)this.lb.listActiveClients().count().toBlocking().first());
    }
    
    @Test
    public void removeClientFromFailure() {
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.immediate());
        
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        this.lb.initialize();
        
        hostEvents.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        Assert.assertEquals(1, (int)this.lb.listActiveClients().count().toBlocking().first());
        
        failureDetector.get(h1).onNext(new Throwable("failed"));
        
        Assert.assertEquals(1, (int)this.lb.listAllClients().count().toBlocking().first());
        Assert.assertEquals(0, (int)this.lb.listActiveClients().count().toBlocking().first());
    }
    

    @Test
    @Ignore
    public void oneBadConnectHost() throws InterruptedException {
        TestClient h1 = TestClient.create("h1", Connects.failure(1, TimeUnit.SECONDS), Behaviors.immediate());
        
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        this.lb.initialize();
        
        hostEvents.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
    }
    
    @Test
    @Ignore
    public void oneBadResponseHost() throws Throwable {
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.failure(1, TimeUnit.SECONDS));

        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        this.lb.initialize();
        
        hostEvents.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb.choose()
            .concatMap(operation)
            .retry()
            .subscribe(response);
        
        response.await(60, TimeUnit.SECONDS);
    }
    
    @Test
    @Ignore
    public void failFirstResponse() throws Throwable {
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.failFirst(1));
        
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        this.lb.initialize();
        
        hostEvents.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb.choose()
            .concatMap(operation)
            .single()
            .retry()
            .subscribe(response);
        
        response.await(60, TimeUnit.SECONDS);
    }
    
    @Test
    @Ignore
    public void retryWithBackoff() {
        Observable
            .error(new RuntimeException("foo"))
            .retryWhen(Retrys.exponentialBackoff(3, 1, TimeUnit.SECONDS))
            .doOnError(RxUtil.error("onError"))
            .toBlocking()
            .first();
    }
    
    @Test
    @Ignore
    public void openConnections() {
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        this.lb.initialize();
        
        Assert.assertEquals(0L,  (long)this.lb.listActiveClients().count().toBlocking().single());
//        Assert.assertEquals(10L, (long)this.selector.prime(10).count().toBlocking().single());
        Assert.assertEquals(10L, (long)this.lb.listActiveClients().count().toBlocking().single());
    }
    
    @Test
    public void splitPools() {
        
    }
    
    @Test
    public void sharedHosts() {
        // vipA : Host1, Host2
        // vipB : Host2, Host3
    }
}
