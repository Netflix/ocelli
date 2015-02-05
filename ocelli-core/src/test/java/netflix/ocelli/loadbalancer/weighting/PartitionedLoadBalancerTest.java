package netflix.ocelli.loadbalancer.weighting;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;
import netflix.ocelli.FailureDetectingInstanceFactory;
import netflix.ocelli.Instance;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.Member;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEventToMember;
import netflix.ocelli.PartitionedLoadBalancer;
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
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observables.GroupedObservable;
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
        
        final FailureDetectingInstanceFactory<TestClient> factory =
                FailureDetectingInstanceFactory.<TestClient>builder()
                .withFailureDetector(failureDetector)
                .build();

        PartitionedLoadBalancer<String, TestClient> plb = new PartitionedLoadBalancer<String, TestClient>();
        
        hostSource
            .compose(new MembershipEventToMember<TestClient>())
            .compose(Member.partitionBy(TestClient.byVip()))
            .map(new Func1<GroupedObservable<String,Member<TestClient>>, GroupedObservable<String, Instance<TestClient>>>() {
                @Override
                public GroupedObservable<String, Instance<TestClient>> call(final GroupedObservable<String, Member<TestClient>> group) {
                    return GroupedObservable.create(group.getKey(), new OnSubscribe<Instance<TestClient>>() {
                        @Override
                        public void call(Subscriber<? super Instance<TestClient>> t1) {
                            group.map(TestClient.memberToInstance(factory)).subscribe(t1);
                        }
                    });
                }
                
            })
            .subscribe(plb);
        
        //////////////////////////
        // Step 1: Add 4 hosts
        
        // Get a LoadBalancer for each partition
        LoadBalancer<TestClient> lbA = plb.get("a");
        LoadBalancer<TestClient> lbB = plb.get("b");
        LoadBalancer<TestClient> lbAll = plb.get("*");
        
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
        final FailureDetectingInstanceFactory<TestClient> factory =
                FailureDetectingInstanceFactory.<TestClient>builder()
                .withFailureDetector(failureDetector)
                .build();
        
        PublishSubject<MembershipEvent<TestClient>> hostSource = PublishSubject.create();
        
        TestClient h2 = TestClient.create("h2", Connects.immediate(), Behaviors.immediate()).withVip("a");
        
        PartitionedLoadBalancer<String, TestClient> plb = new PartitionedLoadBalancer<String, TestClient>();
        
        hostSource
            .compose(new MembershipEventToMember<TestClient>())
            .compose(Member.partitionBy(TestClient.byVip()))
            .map(new Func1<GroupedObservable<String,Member<TestClient>>, GroupedObservable<String, Instance<TestClient>>>() {
                @Override
                public GroupedObservable<String, Instance<TestClient>> call(final GroupedObservable<String, Member<TestClient>> group) {
                    return GroupedObservable.create(group.getKey(), new OnSubscribe<Instance<TestClient>>() {
                        @Override
                        public void call(Subscriber<? super Instance<TestClient>> t1) {
                            group.map(TestClient.memberToInstance(factory)).subscribe(t1);
                        }
                    });
                }
                
            })
            .subscribe(plb);
        
        //////////////////////////
        // Step 1: Add 4 hosts
        
        // Get a LoadBalancer for each partition
        LoadBalancer<TestClient> lbA   = plb.get("a");
        LoadBalancer<TestClient> lbAll = plb.get("*");
        
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
        final FailureDetectingInstanceFactory<TestClient> factory =
                FailureDetectingInstanceFactory.<TestClient>builder()
                .withFailureDetector(failureDetector)
                .build();
        
        PublishSubject<MembershipEvent<TestClient>> hostSource = PublishSubject.create();
        
        TestClient h1 = TestClient.create("h1", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1a");
        TestClient h2 = TestClient.create("h2", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1c");
        TestClient h3 = TestClient.create("h3", Connects.immediate(), Behaviors.immediate()).withRack("us-east-1d");
        
        PartitionedLoadBalancer<String, TestClient> plb = new PartitionedLoadBalancer<String, TestClient>();
        
        hostSource
            .compose(new MembershipEventToMember<TestClient>())
            .compose(Member.partitionBy(TestClient.byRack()))
            .map(new Func1<GroupedObservable<String,Member<TestClient>>, GroupedObservable<String, Instance<TestClient>>>() {
                @Override
                public GroupedObservable<String, Instance<TestClient>> call(final GroupedObservable<String, Member<TestClient>> group) {
                    return GroupedObservable.create(group.getKey(), new OnSubscribe<Instance<TestClient>>() {
                        @Override
                        public void call(Subscriber<? super Instance<TestClient>> t1) {
                            group.map(TestClient.memberToInstance(factory)).subscribe(t1);
                        }
                    });
                }
                
            })
            .subscribe(plb);

        
        hostSource.onNext(MembershipEvent.create(h1, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h2, MembershipEvent.EventType.ADD));
        hostSource.onNext(MembershipEvent.create(h3, MembershipEvent.EventType.ADD));

        LoadBalancer<TestClient> zoneA = plb.get("us-east-1a");
        LoadBalancer<TestClient> zoneB = plb.get("us-east-1b");
        LoadBalancer<TestClient> zoneC = plb.get("us-east-1c");
        LoadBalancer<TestClient> zoneD = plb.get("us-east-1d");
        
        RxUtil.onSubscribeChooseNext(Observable.create(zoneA), Observable.create(zoneB), Observable.create(zoneC), Observable.create(zoneD))
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
