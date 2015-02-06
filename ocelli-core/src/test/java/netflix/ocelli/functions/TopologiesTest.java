package netflix.ocelli.functions;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.CloseableMember;
import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.Member;
import netflix.ocelli.MemberToInstance;
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
        CloseableMember<Integer> m1 = CloseableMember.from(1);
        CloseableMember<Integer> m2 = CloseableMember.from(2);
        CloseableMember<Integer> m3 = CloseableMember.from(3);
        CloseableMember<Integer> m4 = CloseableMember.from(4);
        CloseableMember<Integer> m6 = CloseableMember.from(6);
        CloseableMember<Integer> m7 = CloseableMember.from(7);
        CloseableMember<Integer> m8 = CloseableMember.from(8);
        CloseableMember<Integer> m9 = CloseableMember.from(9);
        CloseableMember<Integer> m10 = CloseableMember.from(10);
        CloseableMember<Integer> m11 = CloseableMember.from(11);
        
        PublishSubject<Member<Integer>> members = PublishSubject.create();
        
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
               .map(MemberToInstance.from(new Func2<Integer, Action0, Instance<Integer>>() {
                    @Override
                    public Instance<Integer> call(Integer key, Action0 shutdown) {
                        return Instance.from(key, BehaviorSubject.create(true));
                    }
               }))
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
