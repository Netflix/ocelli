package netflix.ocelli.selector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.HostEvent;
import netflix.ocelli.HostEvent.EventType;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.Operations;
import netflix.ocelli.client.ResponseObserver;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TestClientFactory;
import netflix.ocelli.client.TestClientMetricsFactory;
import netflix.ocelli.client.TestHost;
import netflix.ocelli.client.TrackingOperation;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import netflix.ocelli.retrys.Retrys;
import netflix.ocelli.selectors.Delays;
import netflix.ocelli.util.Functions;
import netflix.ocelli.util.RxUtil;

import org.junit.After;
import org.junit.Assert;
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
    private static Observable<HostEvent<TestHost>> source;
    
    private DefaultLoadBalancer<TestHost, TestClient> lb;
    private TestClientMetricsFactory<TestHost> metrics = new TestClientMetricsFactory<TestHost>();
    private PublishSubject<HostEvent<TestHost>> hostEvents = PublishSubject.create();

    @Rule
    public TestName testName = new TestName();
    
    @BeforeClass
    public static void setup() {
        List<TestHost> hosts = new ArrayList<TestHost>();
        for (int i = 0; i < NUM_HOSTS; i++) {
            hosts.add(TestHost.create("host-"+i, Connects.immediate(), Behaviors.immediate()));
        }
        
        source = Observable
            .from(hosts)
            .map(HostEvent.<TestHost>toEvent(EventType.ADD));
    }
    
    @After
    public void afterTest() {
        if (this.lb != null) {
            this.lb.shutdown();
        }
    }
    
    @Test
    public void openConnectionImmediately() throws Throwable {
        this.lb = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withHostSource(Observable
                        .just(TestHost.create("h1", Connects.immediate(), Behaviors.immediate()))
                        .map(HostEvent.<TestHost>toEvent(EventType.ADD)))
                .withClientConnector(new TestClientFactory())
                .withConnectedHostCountStrategy(Functions.identity())
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb.choose()
            .concatMap(operation)
            .retry()
            .subscribe(response);
        
        response.await(10, TimeUnit.SECONDS);
        System.out.println(response.get());
        System.out.println(operation.getServers());
    }
    
    @Test
    public void removeClientFromSource() {
        TestHost h1 = TestHost.create("h1", Connects.immediate(), Behaviors.immediate());
        
        this.lb = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withName(testName.getMethodName())
                .withHostSource(hostEvents)
                .withClientConnector(new TestClientFactory())
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        hostEvents.onNext(HostEvent.create(h1, HostEvent.EventType.ADD));
        Assert.assertEquals(1, (int)this.lb.listActiveHosts().count().toBlocking().first());
        
        hostEvents.onNext(HostEvent.create(h1, HostEvent.EventType.REMOVE));
        Assert.assertEquals(0, (int)this.lb.listActiveHosts().count().toBlocking().first());
    }
    
    @Test
    public void removeClientFromFailure() {
        TestHost h1 = TestHost.create("h1", Connects.immediate(), Behaviors.immediate());
        
        this.lb = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withName(testName.getMethodName())
                .withHostSource(hostEvents)
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(metrics)
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        hostEvents.onNext(HostEvent.create(h1, HostEvent.EventType.ADD));
        Assert.assertEquals(1, (int)this.lb.listActiveHosts().count().toBlocking().first());
        
        metrics.get(h1).shutdown();
        
//        hostEvents.onNext(HostEvent.create(h1, HostEvent.EventType.REMOVE));
        
        Assert.assertEquals(0, (int)this.lb.listActiveHosts().count().toBlocking().first());
    }
    

    @Test
    @Ignore
    public void oneBadConnectHost() throws InterruptedException {
        this.lb = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withHostSource(Observable
                        .just(TestHost.create("h1", Connects.failure(1, TimeUnit.SECONDS), Behaviors.immediate()))
                        .map(HostEvent.<TestHost>toEvent(EventType.ADD)))
                .withQuaratineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
                .withClientConnector(new TestClientFactory())
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        TimeUnit.SECONDS.sleep(60);
    }
    
    @Test
    @Ignore
    public void oneBadResponseHost() throws Throwable {
        this.lb = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withHostSource(Observable
                        .just(TestHost.create("bar", Connects.immediate(), Behaviors.failure(1, TimeUnit.SECONDS)))
                        .map(HostEvent.<TestHost>toEvent(EventType.ADD)))
                .withClientConnector(new TestClientFactory())
                .withQuaratineStrategy(Delays.linear(1, TimeUnit.SECONDS))
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
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
        this.lb = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withHostSource(Observable
                        .just(TestHost.create("bar", Connects.immediate(), Behaviors.failFirst(1)))
                        .map(HostEvent.<TestHost>toEvent(EventType.ADD)))
                .withQuaratineStrategy(Delays.linear(1, TimeUnit.SECONDS))
                .withClientConnector(new TestClientFactory())
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb.choose()
            .concatMap(operation)
            .single()
            .retry()
            .subscribe(response);
        
        response.await(60, TimeUnit.SECONDS);
        System.out.println(response.get());
        System.out.println(operation.getServers());
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
        this.lb = DefaultLoadBalancer.<TestHost, TestClient>builder()
                .withHostSource(source)
                .withClientConnector(new TestClientFactory())
                .build();
        
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
