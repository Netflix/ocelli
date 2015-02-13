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
import rx.subjects.BehaviorSubject;
import rx.subjects.PublishSubject;

import com.google.common.collect.Lists;

public class HostToClientPoolTest {
    private final static Logger LOG = LoggerFactory.getLogger(HostToClientPoolTest.class);
    
    @Test
    public void testSimpleConvert() {
        PublishSubject<Instance<Integer>> members1 = PublishSubject.create();
        PublishSubject<Instance<Integer>> members2 = PublishSubject.create();
        
        final ConcurrentMap<Integer, Instance<Integer>> instances = new ConcurrentHashMap<Integer, Instance<Integer>>();
        final AtomicReference<List<Integer>> active = new AtomicReference<List<Integer>>();

        CachingInstanceTransformer<Integer, String> memberToInstance = new IntegerToStringTransformer();
        
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
        
        MutableInstance<Integer> i1 = MutableInstance.from(1);
        MutableInstance<Integer> i2 = MutableInstance.from(2);
        MutableInstance<Integer> i3 = MutableInstance.from(3);
      
        members1.onNext(i1);
        members1.onNext(i2);
        members1.onNext(i3);
        
        members2.onNext(i1);
        
        i1.close();
        i2.close();
        i3.close();
      
        MutableInstance<Integer> i1_2 = MutableInstance.from(1);
        members1.onNext(i1_2);
    }
    
    @Test
    public void testCollector() {
        BehaviorSubject<Boolean> s1 = BehaviorSubject.create();
        Instance<Integer> i1 = MutableInstance.from(1, s1);
        
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
