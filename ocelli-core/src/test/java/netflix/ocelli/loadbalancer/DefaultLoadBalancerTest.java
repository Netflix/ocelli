package netflix.ocelli.loadbalancer;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.CachingInstanceTransformer;
import netflix.ocelli.FailureDetectingInstanceFactory;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.MutableInstance;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.ManualFailureDetector;
import netflix.ocelli.client.Operations;
import netflix.ocelli.client.ResponseObserver;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TestClientConnectorFactory;
import netflix.ocelli.client.TrackingOperation;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Retrys;
import netflix.ocelli.loadbalancer.weighting.LinearWeightingStrategy;
import netflix.ocelli.util.CountDownAction;
import netflix.ocelli.util.RxUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import rx.Observable;
import rx.subjects.PublishSubject;

public class DefaultLoadBalancerTest {
    
    private LoadBalancer<TestClient> lb;
    private PublishSubject<Instance<TestClient>> hostEvents = PublishSubject.create();
    private TestClientConnectorFactory clientConnector = new TestClientConnectorFactory();
    private ManualFailureDetector failureDetector = new ManualFailureDetector();
    
    @Rule
    public TestName testName = new TestName();
    
    @Before 
    public void before() {
        FailureDetectingInstanceFactory<TestClient> factory =
                FailureDetectingInstanceFactory.<TestClient>builder()
                .withQuarantineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
                .withFailureDetector(failureDetector)
                .withClientConnector(clientConnector)
                .build();

        this.lb = RandomWeightedLoadBalancer.create(
                    new LinearWeightingStrategy<TestClient>(
                        TestClient.byPendingRequestCount()));

        hostEvents
        .map(CachingInstanceTransformer.create(factory))  
        .compose(new InstanceCollector<TestClient>())
        .subscribe(lb);
    
    }
    
    @Test
    public void openConnectionImmediately() throws Throwable {
        TestClient client = TestClient.create("h1", Connects.immediate(), Behaviors.immediate());
        
        CountDownAction<TestClient> counter = new CountDownAction<TestClient>(1);
        clientConnector.get(client).stream().subscribe(counter);
        
        hostEvents.onNext(MutableInstance.from(client));
        
        counter.await(1, TimeUnit.SECONDS);
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb
            .concatMap(operation)
            .retry()
            .subscribe(response);
        
        Assert.assertEquals("foo", response.get());
    }
    
    @Test
    public void removeClientFromSource() {
        TestClient client = TestClient.create("h1", Connects.immediate(), Behaviors.immediate());
        
        MutableInstance<TestClient> instance = MutableInstance.from(client);
        
        hostEvents.onNext(instance);
//        Assert.assertEquals(1, (int)this.lb.listActiveClients().count().toBlocking().first());
        
        instance.close();
//        Assert.assertEquals(0, (int)this.lb.listActiveClients().count().toBlocking().first());
    }
    
    @Test
    public void removeClientFromFailure() {
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.immediate());
        
        hostEvents.onNext(MutableInstance.from(h1));
//        Assert.assertEquals(1, (int)this.lb.listActiveClients().count().toBlocking().first());
        
        failureDetector.get(h1).onNext(new Throwable("failed"));
        
//        Assert.assertEquals(1, (int)this.lb.listAllClients().count().toBlocking().first());
//        Assert.assertEquals(0, (int)this.lb.listActiveClients().count().toBlocking().first());
    }
    

    @Test
    @Ignore
    public void oneBadConnectHost() throws InterruptedException {
        TestClient h1 = TestClient.create("h1", Connects.failure(1, TimeUnit.SECONDS), Behaviors.immediate());
        
        hostEvents.onNext(MutableInstance.from(h1));
    }
    
    @Test
    @Ignore
    public void oneBadResponseHost() throws Throwable {
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.failure(1, TimeUnit.SECONDS));

        hostEvents.onNext(MutableInstance.from(h1));
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb
            .concatMap(operation)
            .retry()
            .subscribe(response);
        
        response.await(60, TimeUnit.SECONDS);
    }
    
    @Test
    @Ignore
    public void failFirstResponse() throws Throwable {
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.failFirst(1));
        
        hostEvents.onNext(MutableInstance.from(h1));
        
        TrackingOperation operation = Operations.tracking("foo");
        ResponseObserver response = new ResponseObserver();
        
        lb
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
//        Assert.assertEquals(0L,  (long)this.lb.listActiveClients().count().toBlocking().single());
//        Assert.assertEquals(10L, (long)this.selector.prime(10).count().toBlocking().single());
//        Assert.assertEquals(10L, (long)this.lb.listActiveClients().count().toBlocking().single());
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
