package netflix.ocelli.loadbalancer.weighting;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;
import netflix.ocelli.ClientCollector;
import netflix.ocelli.ClientLifecycleFactory;
import netflix.ocelli.FailureDetectingClientLifecycleFactory;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipPartitioner;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.Connects;
import netflix.ocelli.client.ManualFailureDetector;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import netflix.ocelli.util.RxUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import com.google.common.collect.Sets;

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
        
        ClientLifecycleFactory<TestClient> factory =
                FailureDetectingClientLifecycleFactory.<TestClient>builder()
                    .withFailureDetector(failureDetector)
                    .build();

        MembershipPartitioner<TestClient, String> lb = MembershipPartitioner.create(
                hostSource,
                TestClient.byVip())
                ;

        //////////////////////////
        // Step 1: Add 4 hosts
        
        // Get a LoadBalancer for each partition
        LoadBalancer<TestClient> lbA = RoundRobinLoadBalancer.create(
                lb.getPartition("a").lift(ClientCollector.create(factory)));
        LoadBalancer<TestClient> lbB = RoundRobinLoadBalancer.create(
                lb.getPartition("b").lift(ClientCollector.create(factory)));
        LoadBalancer<TestClient> lbAll = RoundRobinLoadBalancer.create(
                lb.getPartition("*").lift(ClientCollector.create(factory)));
        
        // Add 4 hosts
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h4, MembershipEvent.EventType.ADD));
        
        Assert.assertNotNull(lbA);
        Assert.assertNotNull(lbB);
        Assert.assertNotNull(lbAll);
        
        // List all hosts
        Set<TestClient> hostsA = new HashSet<TestClient>(lbA.all().toList().toBlocking().first());
        Set<TestClient> hostsB = new HashSet<TestClient>(lbB.all().toList().toBlocking().first());
        Set<TestClient> hostsAll = new HashSet<TestClient>(lbAll.all().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2, h3), hostsA);
        Assert.assertEquals(Sets.newHashSet(h3, h4), hostsB);
        Assert.assertEquals(Sets.newHashSet(h1,h2,h3, h4), hostsAll);
        
        //////////////////////////
        // Step 2: Remove h1 host
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.REMOVE));
        
        // List all hosts
        hostsA = new HashSet<TestClient>(lbA.all().toList().toBlocking().first());
        hostsB = new HashSet<TestClient>(lbB.all().toList().toBlocking().first());
        hostsAll = new HashSet<TestClient>(lbAll.all().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2, h3), hostsA);
        Assert.assertEquals(Sets.newHashSet(h3, h4), hostsB);
        Assert.assertEquals(Sets.newHashSet(h2,h3, h4), hostsAll);
        
        //////////////////////////
        // Step 2: Remove h3 host
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.REMOVE));
        
        // List all hosts
        hostsA = new HashSet<TestClient>(lbA.all().toList().toBlocking().first());
        hostsB = new HashSet<TestClient>(lbB.all().toList().toBlocking().first());
        hostsAll = new HashSet<TestClient>(lbAll.all().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2), hostsA);
        Assert.assertEquals(Sets.newHashSet(h4), hostsB);
        Assert.assertEquals(Sets.newHashSet(h2, h4), hostsAll);
        
        // Assert in
        lbA.all().subscribe(RxUtil.info("A:"));
        lbB.all().subscribe(RxUtil.info("B:"));
        lbAll.all().subscribe(RxUtil.info("All:"));
    }
    
    @Test
    public void testVipHostFailure() {
        ClientLifecycleFactory<TestClient> factory =
                FailureDetectingClientLifecycleFactory.<TestClient>builder()
                    .withFailureDetector(failureDetector)
                    .build();
        
        PublishSubject<MembershipEvent<TestClient>> hostSource = PublishSubject.create();
        
        TestClient h2 = TestClient.create("h2", Connects.immediate(), Behaviors.immediate()).withVip("a");
        
        MembershipPartitioner<TestClient, String> lb = MembershipPartitioner.create(
                hostSource,
                TestClient.byVip());

        //////////////////////////
        // Step 1: Add 4 hosts
        
        // Get a LoadBalancer for each partition
        LoadBalancer<TestClient> lbA = RoundRobinLoadBalancer.create(
                lb.getPartition("a").lift(ClientCollector.create(factory)));
        LoadBalancer<TestClient> lbAll = RoundRobinLoadBalancer.create(
                lb.getPartition("*").lift(ClientCollector.create(factory)));
        
        // Add 4 hosts
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        
        // List all hosts
        Set<TestClient> hostsA = new HashSet<TestClient>(lbA.all().toList().toBlocking().first());
        Set<TestClient> hostsAll = new HashSet<TestClient>(lbAll.all().toList().toBlocking().first());

        // Assert initial state
        Assert.assertEquals(Sets.newHashSet(h2), hostsA);
        Assert.assertEquals(Sets.newHashSet(h2), hostsAll);
        
        //////////////////////////
        // Step 2: Kill h2 host
        failureDetector.get(h2).onNext(new Throwable("manual failure"));
        
        // List all hosts
        hostsA = new HashSet<TestClient>(lbA.all().toList().toBlocking().first());
        hostsAll = new HashSet<TestClient>(lbAll.all().toList().toBlocking().first());

        // Assert initial state
        Assert.assertTrue(hostsA.isEmpty());
        Assert.assertTrue(hostsAll.isEmpty());
    }
    
    @Test
    public void retryOnceEachPartition() {
        ClientLifecycleFactory<TestClient> factory =
                FailureDetectingClientLifecycleFactory.<TestClient>builder()
                    .withFailureDetector(failureDetector)
                    .build();
        
        PublishSubject<MembershipEvent<TestClient>> hostSource = PublishSubject.create();
        
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1a");
        TestClient h2 = TestClient.create("h2", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1c");
        TestClient h3 = TestClient.create("h3", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1d");
        
        MembershipPartitioner<TestClient, String> lb = MembershipPartitioner.create(
                hostSource,
                TestClient.byRack())
                ;
        
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.ADD));

        LoadBalancer<TestClient> zoneA = RoundRobinLoadBalancer.create(
                lb.getPartition("us-east-1a").lift(ClientCollector.create(factory)));
        LoadBalancer<TestClient> zoneB = RoundRobinLoadBalancer.create(
                lb.getPartition("us-east-1b").lift(ClientCollector.create(factory)));
        LoadBalancer<TestClient> zoneC = RoundRobinLoadBalancer.create(
                lb.getPartition("us-east-1c").lift(ClientCollector.create(factory)));
        LoadBalancer<TestClient> zoneD = RoundRobinLoadBalancer.create(
                lb.getPartition("us-east-1d").lift(ClientCollector.create(factory)));
        
        RxUtil.onSubscribeChooseNext(zoneA, zoneB, zoneC, zoneD)
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
