package netflix.ocelli.functions;

import java.util.List;

import junit.framework.Assert;
import netflix.ocelli.Host;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;

import org.junit.Test;

import rx.Observable;
import rx.functions.Func1;

import com.google.common.collect.Lists;

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
        int me = 81;
        List<HostWithId> hosts = Lists.newArrayList();
        
        HostWithId h1 = new HostWithId("host10", 8080, 10);
        HostWithId h2 = new HostWithId("host20", 8080, 20);
        HostWithId h3 = new HostWithId("host30", 8080, 30);
        HostWithId h4 = new HostWithId("host40", 8080, 40);
        HostWithId h5 = new HostWithId("host50", 8080, 50);
        HostWithId h6 = new HostWithId("host60", 8080, 60);
        HostWithId h7 = new HostWithId("host70", 8080, 70);
        HostWithId h8 = new HostWithId("host80", 8080, 80);
        HostWithId h9 = new HostWithId("host90", 8080, 90);
        HostWithId h10 = new HostWithId("host100", 8080, 100);
        
        hosts.add(h5);
        hosts.add(h7);
        hosts.add(h1);
        hosts.add(h2);
        hosts.add(h4);
        hosts.add(h3);
        hosts.add(h8);
        hosts.add(h10);
        hosts.add(h9);
        hosts.add(h6);
        
        final List<MembershipEvent<HostWithId>> actual = Observable
            .from(hosts)
            .map(MembershipEvent.<HostWithId>toEvent(EventType.ADD))
            .concatWith(Observable.from(Lists.reverse(hosts)).map(MembershipEvent.<HostWithId>toEvent(EventType.REMOVE)))
            .lift(Topologies.ring(
                me,
                new Func1<HostWithId, Integer>() {
                    @Override
                    public Integer call(HostWithId t1) {
                        return t1.id;
                    }
                },
                new Func1<Integer, Integer>() {
                    @Override
                    public Integer call(Integer t1) {
                        return 2;
                    }
                }))
            .toList().toBlocking().first();
        
        List<MembershipEvent<HostWithId>> expected = Lists.newArrayList(
                MembershipEvent.create(h5, EventType.ADD),
                MembershipEvent.create(h7, EventType.ADD),
                MembershipEvent.create(h1, EventType.ADD),
                MembershipEvent.create(h7, EventType.REMOVE),
                MembershipEvent.create(h2, EventType.ADD),
                MembershipEvent.create(h5, EventType.REMOVE),
                MembershipEvent.create(h10, EventType.ADD),
                MembershipEvent.create(h2, EventType.REMOVE),
                MembershipEvent.create(h9, EventType.ADD),
                MembershipEvent.create(h1, EventType.REMOVE),

                MembershipEvent.create(h1, EventType.ADD),
                MembershipEvent.create(h9, EventType.REMOVE),
                MembershipEvent.create(h2, EventType.ADD),
                MembershipEvent.create(h10, EventType.REMOVE),
                MembershipEvent.create(h5, EventType.ADD),
                MembershipEvent.create(h2, EventType.REMOVE),
                MembershipEvent.create(h7, EventType.ADD),
                MembershipEvent.create(h1, EventType.REMOVE),
                MembershipEvent.create(h7, EventType.REMOVE),
                MembershipEvent.create(h5, EventType.REMOVE)
                );
        
        Assert.assertEquals(actual, expected);
    }
}
