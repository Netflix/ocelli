package netflix.ocelli.functions;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.CachingInstanceTransformer;
import netflix.ocelli.MutableInstance;
import netflix.ocelli.topologies.RingTopology;
import netflix.ocelli.util.RxUtil;

import org.junit.Test;

import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.BehaviorSubject;
import rx.subjects.PublishSubject;

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
    public void test2() {
        MutableInstance<Integer> m1 = MutableInstance.from(1);
        MutableInstance<Integer> m2 = MutableInstance.from(2);
        MutableInstance<Integer> m3 = MutableInstance.from(3);
        MutableInstance<Integer> m4 = MutableInstance.from(4);
        MutableInstance<Integer> m6 = MutableInstance.from(6);
        MutableInstance<Integer> m7 = MutableInstance.from(7);
        MutableInstance<Integer> m8 = MutableInstance.from(8);
        MutableInstance<Integer> m9 = MutableInstance.from(9);
        MutableInstance<Integer> m10 = MutableInstance.from(10);
        MutableInstance<Integer> m11 = MutableInstance.from(11);
        
        PublishSubject<Instance<Integer>> members = PublishSubject.create();
        
        RingTopology<Integer, Integer> mapper = new RingTopology<Integer, Integer>(5, new Func1<Integer, Integer>() {
            @Override
            public Integer call(Integer t1) {
                return t1;
            }
        }, Functions.memoize(3));
        
        AtomicReference<List<Integer>> current = new AtomicReference<List<Integer>>();
        
        members
               .doOnNext(RxUtil.info("add"))
               .compose(mapper)
               .map(CachingInstanceTransformer.<Integer>create())
               .compose(new InstanceCollector<Integer>())
               .doOnNext(RxUtil.info("current"))
               .subscribe(RxUtil.set(current));
               
        members.onNext(m11);
        members.onNext(m7);
        members.onNext(m1);
        members.onNext(m2);
        members.onNext(m4);
        members.onNext(m3);
        members.onNext(m8);
        members.onNext(m10);
        members.onNext(m9);
        members.onNext(m6);

        m6.close();
        m9.close();
        m8.close();
    }
}
