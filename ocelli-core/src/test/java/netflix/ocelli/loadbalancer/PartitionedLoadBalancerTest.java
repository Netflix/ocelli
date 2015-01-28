package netflix.ocelli.loadbalancer;

import com.google.common.collect.Sets;
import junit.framework.Assert;
import netflix.ocelli.LoadBalancers;
import netflix.ocelli.ManagedLoadBalancer;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.ManualFailureDetector;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.util.RxUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import java.util.HashSet;
import java.util.Set;

public class PartitionedLoadBalancerTest {

    @Rule
    public TestName name = new TestName();
    
    private ManualFailureDetector failureDetector = new ManualFailureDetector();
    
    @Test
    public void testVip() throws InterruptedException {
        PublishSubject<MembershipEvent<TestClient>> hostSource = PublishSubject.create();
        
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.immediate());
        TestClient h2 = TestClient.create("h2", Connects.immediate(), Behaviors.immediate()).withVip("a");
        TestClient h3 = TestClient.create("h3", Connects.immediate(), Behaviors.immediate()).withVip("a").withVip("b");
        TestClient h4 = TestClient.create("h4", Connects.immediate(), Behaviors.immediate()).withVip("b");
        
        DefaultPartitioningLoadBalancer<TestClient, String> lb = (DefaultPartitioningLoadBalancer<TestClient, String>) LoadBalancers
                .newBuilder(hostSource)
                .withName(name.getMethodName())
                .withFailureDetector(failureDetector)
                .withPartitioner(TestClient.byVip())
                .build()
                ;

        //////////////////////////
        // Step 1: Add 4 hosts
        
        // Add 4 hosts
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h4, MembershipEvent.EventType.ADD));
        
        // Get a LoadBalancer for each partition
        ManagedLoadBalancer<TestClient> lbA = (ManagedLoadBalancer<TestClient>) lb.get("a");
        ManagedLoadBalancer<TestClient> lbB = (ManagedLoadBalancer<TestClient>) lb.get("b");
        ManagedLoadBalancer<TestClient> lbAll = (ManagedLoadBalancer<TestClient>) lb.get("*");
        
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
        
        DefaultPartitioningLoadBalancer<TestClient, String> lb = (DefaultPartitioningLoadBalancer<TestClient, String>) LoadBalancers
                .<TestClient>newBuilder(hostSource)
                .withName(name.getMethodName())
                .withFailureDetector(failureDetector)
                .withPartitioner(TestClient.byVip())
                .build()
                ;

        //////////////////////////
        // Step 1: Add 4 hosts
        
        // Add 4 hosts
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        
        // Get a LoadBalancer for each partition
        ManagedLoadBalancer<TestClient> lbA = (ManagedLoadBalancer<TestClient>) lb.get("a");
        ManagedLoadBalancer<TestClient> lbAll = (ManagedLoadBalancer<TestClient>) lb.get("*");
        
        // List all hosts
        Set<TestClient> hostsA = new HashSet<TestClient>(lbA.listAllClients().toList().toBlocking().first());
        Set<TestClient> hostsAll = new HashSet<TestClient>(lbAll.listAllClients().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2), hostsA);
        Assert.assertEquals(Sets.newHashSet(h2), hostsAll);
        
        //////////////////////////
        // Step 2: Kill h2 host
        failureDetector.get(h2).onNext(new Throwable("manual failure"));
        
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
        
        DefaultPartitioningLoadBalancer<TestClient, String> lb = (DefaultPartitioningLoadBalancer<TestClient, String>) LoadBalancers
                .newBuilder(hostSource)
                .withName(name.getMethodName())
                .withPartitioner(TestClient.byRack())
                .build()
                ;
        
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.ADD));

        ManagedLoadBalancer<TestClient> zoneA = (ManagedLoadBalancer<TestClient>) lb.get("us-east-1a");
        ManagedLoadBalancer<TestClient> zoneB = (ManagedLoadBalancer<TestClient>) lb.get("us-east-1b");
        ManagedLoadBalancer<TestClient> zoneC = (ManagedLoadBalancer<TestClient>) lb.get("us-east-1c");
        ManagedLoadBalancer<TestClient> zoneD = (ManagedLoadBalancer<TestClient>) lb.get("us-east-1d");
        
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
