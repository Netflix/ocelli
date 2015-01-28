package netflix.ocelli.execute;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import netflix.ocelli.LoadBalancerBuilder;
import netflix.ocelli.LoadBalancers;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.ManualFailureDetector;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TestClientConnectorFactory;
import netflix.ocelli.execute.BackupRequestExecutionStrategy;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Functions;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import netflix.ocelli.selectors.RoundRobinSelector;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class BackupRequestExecutionStrategyTest {
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
        builder = LoadBalancers.newBuilder(hostEvents)
            .withName("Test-" + testName.getMethodName())
            .withActiveClientCountStrategy(Functions.identity())
            .withQuarantineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
            .withFailureDetector(failureDetector)
            .withClientConnector(clientConnector)
            .withSelectionStrategy(
                new RoundRobinSelector<TestClient>());
    }
    
    @After
    public void afterTest() {
        if (this.lb != null) {
            this.lb.shutdown();
        }
    }
    
    @Test
    public void testNoTimeout() throws InterruptedException {
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        
        source.subscribe(hostEvents);
        
        final AtomicInteger counter = new AtomicInteger();
        
        String id = BackupRequestExecutionStrategy.create(
                lb, 
                new Func0<Integer>() {
                    @Override
                    public Integer call() {
                        return 10;
                    }
                })
                .execute(
                new Func1<TestClient, Observable<String>>() {
                    @Override
                    public Observable<String> call(TestClient client) {
                        counter.incrementAndGet();                        
                        return Observable.just(client.id()).delay(5, TimeUnit.MILLISECONDS);
                    }
                })
                .toBlocking()
                .first();
        
        Assert.assertEquals("host-1", id);
        Assert.assertEquals(1, counter.get());
    }
    
    @Test
    public void testSecondIsFaster() throws InterruptedException {
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        
        source.subscribe(hostEvents);
        
        final AtomicInteger counter = new AtomicInteger();
        final long[] delays = new long[]{30, 10};
        
        String id = BackupRequestExecutionStrategy.create(
                lb, 
                new Func0<Integer>() {
                    @Override
                    public Integer call() {
                        return 10;
                    }
                    
                })
                .execute(new Func1<TestClient, Observable<String>>() {
                    @Override
                    public Observable<String> call(TestClient client) {
                        return Observable.just(client.id()).delay(delays[counter.getAndIncrement()], TimeUnit.MILLISECONDS);
                    }
                })
                .toBlocking()
                .first();
        
        Assert.assertEquals("host-2", id);
        Assert.assertEquals(2, counter.get());
    }
    
    @Test(expected=RuntimeException.class)
    public void testOverallTimeout() throws InterruptedException {
        this.lb = (DefaultLoadBalancer<TestClient>) builder.build();
        
        source.subscribe(hostEvents);
        
        final AtomicInteger counter = new AtomicInteger();
        final long[] delays = new long[]{50, 50};
        
        BackupRequestExecutionStrategy.create(
                lb, 
                new Func0<Integer>() {
                    @Override
                    public Integer call() {
                        return 10;
                    }
                    
                })
                .execute(
                    new Func1<TestClient, Observable<String>>() {
                        @Override
                        public Observable<String> call(TestClient client) {
                            return Observable.just(client.id()).delay(delays[counter.getAndIncrement()], TimeUnit.MILLISECONDS);
                        }
                    })
                .timeout(21, TimeUnit.MILLISECONDS)
                .toBlocking()
                .first();
        
        Assert.assertEquals(2, counter.get());
    }
    
    @Test
    public void testOneFailover() {
        builder.build();
    }
}
