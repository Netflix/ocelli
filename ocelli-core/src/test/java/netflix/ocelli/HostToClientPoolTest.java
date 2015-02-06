package netflix.ocelli;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;
import netflix.ocelli.util.RxUtil;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscription;
import rx.functions.Action1;
import rx.subjects.PublishSubject;

import com.google.common.collect.Lists;

public class HostToClientPoolTest {
    private final static Logger LOG = LoggerFactory.getLogger(HostToClientPoolTest.class);
    
    @Test
    public void testSimpleConvert() {
        PublishSubject<Member<Integer>> members1 = PublishSubject.create();
        PublishSubject<Member<Integer>> members2 = PublishSubject.create();
        
        final ConcurrentMap<Integer, Instance<Integer>> instances = new ConcurrentHashMap<Integer, Instance<Integer>>();
        final AtomicReference<List<Integer>> active = new AtomicReference<List<Integer>>();

        MemberToInstance<Integer, String> memberToInstance = MemberToInstance.from(new IntegerToStringLifecycleFactory());
        
        members1
            .doOnNext(RxUtil.info("member1:   "))
            .map(memberToInstance)
            .doOnNext(RxUtil.info("instance1: " ))
            .compose(new InstanceCollector<String>())
            .doOnNext(RxUtil.info("active1:   "))
            .subscribe();

        members2
            .doOnNext(RxUtil.info("member2:   "))
            .map(memberToInstance)
            .doOnNext(RxUtil.info("instance2: " ))
            .compose(new InstanceCollector<String>())
            .doOnNext(RxUtil.info("active2:   "))
            .subscribe();
        
        CloseableMember<Integer> i1 = CloseableMember.from(1);
        CloseableMember<Integer> i2 = CloseableMember.from(2);
        CloseableMember<Integer> i3 = CloseableMember.from(3);
      
        members1.onNext(i1);
        members1.onNext(i2);
        members1.onNext(i3);
        
        members2.onNext(i1);
        
        i1.close();
        i2.close();
        i3.close();
      
        CloseableMember<Integer> i1_2 = CloseableMember.from(1);
        members1.onNext(i1_2);
    }
    
    @Test
    public void testCollector() {
        PublishSubject<Boolean> s1 = PublishSubject.create();
        Instance<Integer> i1 = new Instance<Integer>(1, s1);
        
        PublishSubject<Instance<Integer>> source = PublishSubject.create();
        
        final AtomicReference<List<Integer>> active = new AtomicReference<List<Integer>>();
        
        Subscription s = source
                .compose(new InstanceCollector<Integer>())
                .subscribe(new Action1<List<Integer>>() {
                    @Override
                    public void call(List<Integer> t1) {
                        active.set(t1);
                    }
                });
        
        source.onNext(i1);
        
        s1.onNext(true);
        Assert.assertEquals(Lists.newArrayList(1), active.get());
        s1.onNext(false);
        Assert.assertEquals(Lists.newArrayList(), active.get());
        s1.onNext(true);
        Assert.assertEquals(Lists.newArrayList(1), active.get());
        s1.onCompleted();
        Assert.assertEquals(Lists.newArrayList(), active.get());
        s1.onNext(true);
        Assert.assertEquals(Lists.newArrayList(), active.get());
        
        s.unsubscribe();
    }
}
