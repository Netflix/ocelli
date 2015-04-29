package netflix.ocelli.toplogies;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;
import netflix.ocelli.CloseableInstance;
import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.functions.Functions;
import netflix.ocelli.topologies.RingTopology;
import netflix.ocelli.util.RxUtil;

import org.junit.Test;

import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import com.google.common.collect.Sets;

public class TopologiesTest {
    public static class HostWithId extends Host {
        private final Integer id;
        
        public HostWithId(String hostName, int port, Integer id) {
            super(hostName, port);
            this.id = id;
        }

        public Integer getId() {
            return this.id;
        }
        
        public String toString() {
            return id.toString();
        }
    }
    
    @Test
    public void test() {
        CloseableInstance<Integer> m1 = CloseableInstance.from(1);
        CloseableInstance<Integer> m2 = CloseableInstance.from(2);
        CloseableInstance<Integer> m3 = CloseableInstance.from(3);
        CloseableInstance<Integer> m4 = CloseableInstance.from(4);
        CloseableInstance<Integer> m6 = CloseableInstance.from(6);
        CloseableInstance<Integer> m7 = CloseableInstance.from(7);
        CloseableInstance<Integer> m8 = CloseableInstance.from(8);
        CloseableInstance<Integer> m9 = CloseableInstance.from(9);
        CloseableInstance<Integer> m10 = CloseableInstance.from(10);
        CloseableInstance<Integer> m11 = CloseableInstance.from(11);
        
        PublishSubject<Instance<Integer>> members = PublishSubject.create();
        
        TestScheduler scheduler = new TestScheduler();
        
        RingTopology<Integer, Integer> mapper = RingTopology.create(5, Functions.identity(), Functions.memoize(3), scheduler);
        
        AtomicReference<List<Integer>> current = new AtomicReference<List<Integer>>();
        
        members
               .doOnNext(RxUtil.info("add"))
               .compose(mapper)
               .compose(new InstanceCollector<Integer>())
               .doOnNext(RxUtil.info("current"))
               .subscribe(RxUtil.set(current));
               
        members.onNext(m11);
        Assert.assertEquals(Sets.newHashSet(11)       , Sets.newHashSet(current.get()));
        members.onNext(m7);
        Assert.assertEquals(Sets.newHashSet(7, 11)    , Sets.newHashSet(current.get()));
        members.onNext(m1);
        Assert.assertEquals(Sets.newHashSet(1, 7, 11) , Sets.newHashSet(current.get()));
        members.onNext(m2);
        Assert.assertEquals(Sets.newHashSet(1, 7, 11) , Sets.newHashSet(current.get()));
        members.onNext(m4);
        Assert.assertEquals(Sets.newHashSet(1, 7, 11) , Sets.newHashSet(current.get()));
        members.onNext(m3);
        Assert.assertEquals(Sets.newHashSet(1, 7, 11) , Sets.newHashSet(current.get()));
        members.onNext(m8);
        Assert.assertEquals(Sets.newHashSet(7, 8, 11) , Sets.newHashSet(current.get()));
        members.onNext(m10);
        Assert.assertEquals(Sets.newHashSet(7, 8, 10),  Sets.newHashSet(current.get()));
        members.onNext(m9);
        Assert.assertEquals(Sets.newHashSet(7, 8, 9),   Sets.newHashSet(current.get()));
        members.onNext(m6);
        Assert.assertEquals(Sets.newHashSet(6, 7, 8),   Sets.newHashSet(current.get()));

        m6.close();
        Assert.assertEquals(Sets.newHashSet(7, 8, 9),   Sets.newHashSet(current.get()));
        m9.close();
        Assert.assertEquals(Sets.newHashSet(7, 8, 10),  Sets.newHashSet(current.get()));
        m8.close();
        Assert.assertEquals(Sets.newHashSet(7, 10, 11), Sets.newHashSet(current.get()));
    }
}
