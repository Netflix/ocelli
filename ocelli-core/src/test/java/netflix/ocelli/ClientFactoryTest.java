package netflix.ocelli;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.util.RxUtil;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.subjects.PublishSubject;

import com.google.common.collect.Lists;

public class ClientFactoryTest {
    private final static Logger LOG = LoggerFactory.getLogger(ClientFactoryTest.class);
    
    @Test
    public void testAddAndRemove() {
        MemberToInstance<Integer, String> memberToInstance = MemberToInstance.from(new IntegerToStringLifecycleFactory());

        PublishSubject<MembershipEvent<Integer>> events = PublishSubject.create();
        
        final AtomicReference<List<String>> current = new AtomicReference<List<String>>();
        
        events
            .compose(new MembershipEventToMember<Integer>())
            .map(memberToInstance)
            .compose(new InstanceCollector<String>())
            .subscribe(RxUtil.set(current));

        events.onNext(MembershipEvent.create(0, EventType.ADD));
        Assert.assertEquals(Lists.newArrayList("Client-0"), current.get());
        events.onNext(MembershipEvent.create(1, EventType.ADD));
        Assert.assertEquals(Lists.newArrayList("Client-1", "Client-0"), current.get());
        events.onNext(MembershipEvent.create(1, EventType.ADD));
        Assert.assertEquals(Lists.newArrayList("Client-1", "Client-0"), current.get());
        events.onNext(MembershipEvent.create(1, EventType.REMOVE));
        Assert.assertEquals(Lists.newArrayList("Client-0"), current.get());
        events.onNext(MembershipEvent.create(1, EventType.ADD));
        Assert.assertEquals(Lists.newArrayList("Client-1", "Client-0"), current.get());
        events.onNext(MembershipEvent.create(0, EventType.REMOVE));
        Assert.assertEquals(Lists.newArrayList("Client-1"), current.get());
        events.onNext(MembershipEvent.create(1, EventType.REMOVE));
        Assert.assertEquals(Lists.newArrayList(), current.get());
    }
    
    @Test
    public void testReferenceCounting() {
        IntegerToStringLifecycleFactory lifecycle = new IntegerToStringLifecycleFactory();
        
        MemberToInstance<Integer, String> memberToInstance = MemberToInstance.from(lifecycle);
        
        PublishSubject<MembershipEvent<Integer>> events = PublishSubject.create();
        
        final AtomicReference<List<String>> lb1Clients = new AtomicReference<List<String>>();
        final AtomicReference<List<String>> lb2Clients = new AtomicReference<List<String>>();
        
        events
            .compose(new MembershipEventToMember<Integer>())
            .map(memberToInstance)
            .compose(new InstanceCollector<String>())
            .subscribe(RxUtil.set(lb1Clients));

        events
            .compose(new MembershipEventToMember<Integer>())
            .map(memberToInstance)
            .compose(new InstanceCollector<String>())
            .subscribe(RxUtil.set(lb2Clients));
        
        Assert.assertEquals(0,  lifecycle.added().size());
        events.onNext(MembershipEvent.create(0, EventType.ADD));
        Assert.assertEquals(Lists.<String>newArrayList("Client-0"), lb1Clients.get());
        Assert.assertEquals(Lists.<String>newArrayList("Client-0"), lb2Clients.get());
        Assert.assertEquals(0,  lifecycle.removed().size());
        events.onNext(MembershipEvent.create(0, EventType.REMOVE));
        Assert.assertEquals(Lists.<String>newArrayList(), lb1Clients.get());
        Assert.assertEquals(Lists.<String>newArrayList(), lb2Clients.get());
        Assert.assertEquals(1,  lifecycle.removed().size());
        events.onNext(MembershipEvent.create(0, EventType.ADD));
        Assert.assertEquals(Lists.<String>newArrayList("Client-0"), lb1Clients.get());
        Assert.assertEquals(Lists.<String>newArrayList("Client-0"), lb2Clients.get());
        Assert.assertEquals(1,  lifecycle.removed().size());
        Assert.assertEquals(2,  lifecycle.added().size());
        events.onNext(MembershipEvent.create(0, EventType.REMOVE));
        Assert.assertEquals(Lists.<String>newArrayList(), lb1Clients.get());
        Assert.assertEquals(Lists.<String>newArrayList(), lb2Clients.get());
        Assert.assertEquals(2,  lifecycle.removed().size());
        Assert.assertEquals(2,  lifecycle.added().size());
    }
}
