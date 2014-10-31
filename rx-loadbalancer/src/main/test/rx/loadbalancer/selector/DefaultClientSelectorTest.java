package rx.loadbalancer.selector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.TestClientFactory;
import rx.loadbalancer.client.Behaviors;
import rx.loadbalancer.client.Connects;
import rx.loadbalancer.client.Operations;
import rx.loadbalancer.client.ResponseObserver;
import rx.loadbalancer.client.TestClient;
import rx.loadbalancer.client.TestHost;
import rx.loadbalancer.client.TrackingOperation;
import rx.loadbalancer.loadbalancer.DefaultLoadBalancer;
import rx.loadbalancer.metrics.ClientMetrics;
import rx.loadbalancer.metrics.SimpleClientMetricsFactory;

public class DefaultClientSelectorTest {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultClientSelectorTest.class);
    
    private static final int NUM_HOSTS = 10;
    private static Observable<HostEvent<TestHost>> source;
    
    private DefaultLoadBalancer<TestHost, TestClient, ClientMetrics> selector;
    
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
        if (this.selector != null) {
            this.selector.shutdown();
        }
    }
    
    @Test
    public void openConnectionImmediately() throws Throwable {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                        .just(TestHost.create("test", Connects.immediate(), Behaviors.immediate()))
                        .map(HostEvent.<TestHost>toAdd()))
                .withConnector(new TestClientFactory())
                .withClientTrackerFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.selector.events().subscribe(new Action1<HostEvent<TestHost>>() {
            @Override
            public void call(HostEvent<TestHost> event) {
                LOG.info(event.toString());
            }
        });
        
        this.selector.initialize();
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        selector.select()
            .concatMap(operation)
            .retry()
            .subscribe(response);
        
        response.await(10, TimeUnit.SECONDS);
        System.out.println(response.get());
        System.out.println(operation.getServers());
    }
    
    @Test
    public void oneBadConnectHost() throws InterruptedException {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                        .just(TestHost.create("bar", Connects.failure(1, TimeUnit.SECONDS), Behaviors.immediate()))
                        .map(HostEvent.<TestHost>toAdd()))
                .withConnector(new TestClientFactory())
                .withQuaratineStrategy(new Func1<Integer, Long>() {
                    @Override
                    public Long call(Integer t1) {
                        return 1000L * t1;
                    }
                })
                .withClientTrackerFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.selector.events().subscribe(new Action1<HostEvent<TestHost>>() {
            @Override
            public void call(HostEvent<TestHost> event) {
                LOG.info(event.toString());
            }
        });
        
        this.selector.initialize();
        
        TimeUnit.SECONDS.sleep(60);
    }
    
    @Test
    public void oneBadResponseHost() throws Throwable {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                        .just(TestHost.create("bar", Connects.immediate(), Behaviors.failure(1, TimeUnit.SECONDS)))
                        .map(HostEvent.<TestHost>toAdd()))
                .withConnector(new TestClientFactory())
                .withQuaratineStrategy(new Func1<Integer, Long>() {
                    @Override
                    public Long call(Integer t1) {
                        return 1000L * t1;
                    }
                })
                .withClientTrackerFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.selector.events().subscribe(new Action1<HostEvent<TestHost>>() {
            @Override
            public void call(HostEvent<TestHost> event) {
                LOG.info(event.toString());
            }
        });
        
        this.selector.initialize();
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        selector.select()
            .concatMap(operation)
            .retry()
            .subscribe(response);
        
        response.await(60, TimeUnit.SECONDS);
    }
    
    @Test
    public void failFirstResponse() throws Throwable {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(Observable
                        .just(TestHost.create("bar", Connects.immediate(), Behaviors.failFirst(1)))
                        .map(HostEvent.<TestHost>toAdd()))
                .withConnector(new TestClientFactory())
                .withQuaratineStrategy(new Func1<Integer, Long>() {
                    @Override
                    public Long call(Integer t1) {
                        return 1000L * t1;
                    }
                })
                .withClientTrackerFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.selector.events().subscribe(new Action1<HostEvent<TestHost>>() {
            @Override
            public void call(HostEvent<TestHost> event) {
                LOG.info(event.toString());
            }
        });
        
        this.selector.initialize();
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        selector.select()
            .concatMap(operation)
            .single()
            .retry()
            .subscribe(response);
        
        response.await(60, TimeUnit.SECONDS);
        System.out.println(response.get());
        System.out.println(operation.getServers());
    }
    
    @Test
    public void openConnections() {
        this.selector = DefaultLoadBalancer.<TestHost, TestClient, ClientMetrics>builder()
                .withHostSource(source)
                .withConnector(new TestClientFactory())
                .withClientTrackerFactory(new SimpleClientMetricsFactory<TestHost>())
                .build();
        
        this.selector.initialize();
        
        Assert.assertEquals(0L,  (long)this.selector.listActiveClients().count().toBlocking().single());
//        Assert.assertEquals(10L, (long)this.selector.prime(10).count().toBlocking().single());
        Assert.assertEquals(10L, (long)this.selector.listActiveClients().count().toBlocking().single());
    }
}
