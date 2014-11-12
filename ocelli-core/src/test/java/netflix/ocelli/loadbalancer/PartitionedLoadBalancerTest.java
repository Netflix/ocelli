package netflix.ocelli.loadbalancer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import netflix.ocelli.ManagedLoadBalancer;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.PartitionedLoadBalancer;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.loadbalancer.DefaultLoadBalancer;
import netflix.ocelli.loadbalancer.DefaultPartitioningLoadBalancer;
import netflix.ocelli.util.RxUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import com.google.common.collect.Sets;

public class PartitionedLoadBalancerTest {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionedLoadBalancerTest.class);
    
    @Rule
    public TestName name = new TestName();
    
    @Test
    public void testVip() throws InterruptedException {
        PublishSubject<MembershipEvent<TestClient>> hostSource = PublishSubject.create();
        
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.immediate());
        TestClient h2 = TestClient.create("h2", Connects.immediate(), Behaviors.immediate()).withVip("a");
        TestClient h3 = TestClient.create("h3", Connects.immediate(), Behaviors.immediate()).withVip("a").withVip("b");
        TestClient h4 = TestClient.create("h4", Connects.immediate(), Behaviors.immediate()).withVip("b");
        
        DefaultPartitioningLoadBalancer<TestClient, TestClient, String> lb = DefaultPartitioningLoadBalancer.<TestClient, TestClient, String>builder()
                .withName(name.getMethodName())
                .withHostSource(hostSource)
                .withPartitioner(TestClient.byVip())
                .withMetricsConnector(new Func1<TestClient, Observable<TestClient>>() {
                    @Override
                    public Observable<TestClient> call(TestClient t1) {
                        return Observable.just(t1);
                    }
                })
                .build()
                ;
        lb.initialize();

        //////////////////////////
        // Step 1: Add 4 hosts
        
        // Add 4 hosts
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h4, MembershipEvent.EventType.ADD));
        
        TimeUnit.SECONDS.sleep(10);
        
        // Get a LoadBalancer for each partition
        ManagedLoadBalancer<TestClient> lbA = lb.get("a");
        ManagedLoadBalancer<TestClient> lbB = lb.get("b");
        ManagedLoadBalancer<TestClient> lbAll = lb.get("*");
        
        Assert.assertNotNull(lbA);
        Assert.assertNotNull(lbB);
        Assert.assertNotNull(lbAll);
        
        // List all hosts
        Set<TestClient> hostsA = new HashSet<TestClient>(lbA.listAllClients().toList().toBlocking().first());
        Set<TestClient> hostsB = new HashSet<TestClient>(lbB.listAllClients().toList().toBlocking().first());
        Set<TestClient> hostsAll = new HashSet<TestClient>(lbAll.listAllClients().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2, h3), hostsA);
        Assert.assertEquals(Sets.newHashSet(h3, h4), hostsB);
        Assert.assertEquals(Sets.newHashSet(h1,h2,h3, h4), hostsAll);
        
        //////////////////////////
        // Step 2: Remove h1 host
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.REMOVE));
        
        // List all hosts
        hostsA = new HashSet<TestClient>(lbA.listAllClients().toList().toBlocking().first());
        hostsB = new HashSet<TestClient>(lbB.listAllClients().toList().toBlocking().first());
        hostsAll = new HashSet<TestClient>(lbAll.listAllClients().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2, h3), hostsA);
        Assert.assertEquals(Sets.newHashSet(h3, h4), hostsB);
        Assert.assertEquals(Sets.newHashSet(h2,h3, h4), hostsAll);
        
        //////////////////////////
        // Step 2: Remove h3 host
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.REMOVE));
        
        // List all hosts
        hostsA = new HashSet<TestClient>(lbA.listAllClients().toList().toBlocking().first());
        hostsB = new HashSet<TestClient>(lbB.listAllClients().toList().toBlocking().first());
        hostsAll = new HashSet<TestClient>(lbAll.listAllClients().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2), hostsA);
        Assert.assertEquals(Sets.newHashSet(h4), hostsB);
        Assert.assertEquals(Sets.newHashSet(h2, h4), hostsAll);
        
        // Assert in
        lbA.listAllClients().subscribe(RxUtil.info("A:"));
        lbB.listAllClients().subscribe(RxUtil.info("B:"));
        lbAll.listAllClients().subscribe(RxUtil.info("All:"));
    }
    
    @Test
    public void testVipHostFailure() {
        PublishSubject<MembershipEvent<TestClient>> hostSource = PublishSubject.create();
        
        TestClient h2 = TestClient.create("h2", Connects.immediate(), Behaviors.immediate()).withVip("a");
        
        DefaultPartitioningLoadBalancer<TestClient, TestClient, String> lb = DefaultPartitioningLoadBalancer.<TestClient, TestClient, String>builder()
                .withMetricsConnector(new Func1<TestClient, Observable<TestClient>>() {
                    @Override
                    public Observable<TestClient> call(TestClient t1) {
                        return Observable.just(t1);
                    }
                })
                .withName(name.getMethodName())
                .withHostSource(hostSource)
                .withPartitioner(TestClient.byVip())
                .build()
                ;
        lb.initialize();

        //////////////////////////
        // Step 1: Add 4 hosts
        
        // Add 4 hosts
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        
        // Get a LoadBalancer for each partition
        ManagedLoadBalancer<TestClient> lbA = lb.get("a");
        ManagedLoadBalancer<TestClient> lbAll = lb.get("*");
        
        // List all hosts
        Set<TestClient> hostsA = new HashSet<TestClient>(lbA.listAllClients().toList().toBlocking().first());
        Set<TestClient> hostsAll = new HashSet<TestClient>(lbAll.listAllClients().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2), hostsA);
        Assert.assertEquals(Sets.newHashSet(h2), hostsAll);
        
        //////////////////////////
        // Step 2: Kill h2 host
//        TestHostMetrics m = lb.getClient(h2).getMetrics(TestHostMetrics.class);
//        Assert.assertNotNull(m);
//        m.shutdown();
        
        // List all hosts
        hostsA = new HashSet<TestClient>(lbA.listActiveClients().toList().toBlocking().first());
        hostsAll = new HashSet<TestClient>(lbAll.listActiveClients().toList().toBlocking().first());

        // Assert initial state
        Assert.assertTrue(hostsA.isEmpty());
        Assert.assertTrue(hostsAll.isEmpty());
    }
    
    @Test
    public void retryOnceEachPartition() {
        PublishSubject<MembershipEvent<TestClient>> hostSource = PublishSubject.create();
        
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1a");
        TestClient h2 = TestClient.create("h2", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1c");
        TestClient h3 = TestClient.create("h3", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1d");
        
        DefaultPartitioningLoadBalancer<TestClient, TestClient, String> lb = DefaultPartitioningLoadBalancer.<TestClient, TestClient, String>builder()
                .withName(name.getMethodName())
                .withHostSource(hostSource)
                .withPartitioner(TestClient.byRack())
                .withMetricsConnector(new Func1<TestClient, Observable<TestClient>>() {
                    @Override
                    public Observable<TestClient> call(TestClient t1) {
                        return Observable.just(t1);
                    }
                })
                .build()
                ;
        
        lb.initialize();
        
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.ADD));

        ManagedLoadBalancer<TestClient> zoneA = lb.get("us-east-1a");
        ManagedLoadBalancer<TestClient> zoneB = lb.get("us-east-1b");
        ManagedLoadBalancer<TestClient> zoneC = lb.get("us-east-1c");
        ManagedLoadBalancer<TestClient> zoneD = lb.get("us-east-1d");
        
        RxUtil.onSubscribeChooseNext(zoneA.choose(), zoneB.choose(), zoneC.choose(), zoneD.choose())
            .concatMap(new Func1<Observable<TestClient>, Observable<String>>() {
                @Override
                public Observable<String> call(Observable<TestClient> t1) {
                    return t1
                        .concatMap(new Func1<TestClient, Observable<String>>() {
                            @Override
                            public Observable<String> call(TestClient t1) {
                                return Observable.error(new Throwable(t1.toString()));
                            }
                        })
                        .single();
                }
            })
            .doOnError(RxUtil.error("Got error: "))
            .retry()
            .subscribe();
            ;
    }
    
    @Test
    public void testShard() {
        
    }
    
    @Test
    public void testConsistentHash() {
        
    }
    
    @Test
    public void testRack() {
        
    }
}
