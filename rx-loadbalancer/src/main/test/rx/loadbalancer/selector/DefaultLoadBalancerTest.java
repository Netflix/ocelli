package rx.loadbalancer.selector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.ManagedClientFactory;
import rx.loadbalancer.client.Behaviors;
import rx.loadbalancer.client.Connects;
import rx.loadbalancer.client.Operations;
import rx.loadbalancer.client.ResponseObserver;
import rx.loadbalancer.client.TestClient;
import rx.loadbalancer.client.TestClientFactory;
import rx.loadbalancer.client.TestClientMetricsFactory;
import rx.loadbalancer.client.TestHost;
import rx.loadbalancer.client.TrackingOperation;
import rx.loadbalancer.loadbalancer.DefaultLoadBalancer;
import rx.loadbalancer.metrics.ClientMetrics;
import rx.loadbalancer.metrics.SimpleClientMetricsFactory;
import rx.loadbalancer.retrys.Retrys;
import rx.loadbalancer.selectors.Delays;
import rx.loadbalancer.util.Functions;
import rx.loadbalancer.util.RxUtil;
import rx.subjects.PublishSubject;

public class DefaultLoadBalancerTest {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancerTest.class);
    
    private static final int NUM_HOSTS = 10;
    private static Observable<HostEvent<TestHost>> source;
    
    private DefaultLoadBalancer<TestHost, TestClient, ClientMetrics> lb;
    private TestClientMetricsFactory<TestHost> metrics = new TestClientMetricsFactory<TestHost>();
    private TestClientFactory clientFactory = new TestClientFactory();
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
            .map(HostEvent.<TestHost>toAdd());
    }
    
    @After
    public void afterTest() {
        if (this.lb != null) {
            this.lb.shutdown();
        }
    }
    
    @Test
    public void openConnectionImmediately() throws Throwable {
        this.lb = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                        .just(TestHost.create("h1", Connects.immediate(), Behaviors.immediate()))
                        .map(HostEvent.<TestHost>toAdd()))
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
                .withConnectedHostCountStrategy(Functions.identity())
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb.select()
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
        
        this.lb = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withName(testName.getMethodName())
                .withHostSource(hostEvents)
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
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
        
        this.lb = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withName(testName.getMethodName())
                .withHostSource(hostEvents)
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
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
        this.lb = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                        .just(TestHost.create("h1", Connects.failure(1, TimeUnit.SECONDS), Behaviors.immediate()))
                        .map(HostEvent.<TestHost>toAdd()))
                .withQuaratineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        TimeUnit.SECONDS.sleep(60);
    }
    
    @Test
    @Ignore
    public void oneBadResponseHost() throws Throwable {
        this.lb = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                        .just(TestHost.create("bar", Connects.immediate(), Behaviors.failure(1, TimeUnit.SECONDS)))
                        .map(HostEvent.<TestHost>toAdd()))
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
                .withQuaratineStrategy(Delays.linear(1, TimeUnit.SECONDS))
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb.select()
            .concatMap(operation)
            .retry()
            .subscribe(response);
        
        response.await(60, TimeUnit.SECONDS);
    }
    
    @Test
    @Ignore
    public void failFirstResponse() throws Throwable {
        this.lb = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                        .just(TestHost.create("bar", Connects.immediate(), Behaviors.failFirst(1)))
                        .map(HostEvent.<TestHost>toAdd()))
                .withQuaratineStrategy(Delays.linear(1, TimeUnit.SECONDS))
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.lb.events().subscribe(RxUtil.info(""));
        this.lb.initialize();
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb.select()
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
        this.lb = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(source)
                .withClientConnector(new TestClientFactory())
                .withMetricsFactory(new SimpleClientMetricsFactory<TestHost>())
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
