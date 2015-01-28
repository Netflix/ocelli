package netflix.ocelli;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;
import netflix.ocelli.MembershipEvent.EventType;

import org.junit.Test;

import com.google.common.collect.Lists;

import rx.functions.Action1;
import rx.subjects.PublishSubject;

public class ClientFactoryTest {
    @Test
    public void testAddAndRemove() {
        HostToClientCachingLifecycleFactory<Integer, String> factory 
            = new HostToClientCachingLifecycleFactory<Integer, String>(
                new HostToClient<Integer, String>() {
                    public String call(Integer host) {
                        return "Client-" + host;
                    }
                },
                FailureDetectingClientLifecycleFactory.<String>builder()
                    .build());

        PublishSubject<MembershipEvent<Integer>> events = PublishSubject.create();
        
        events
            .lift(new HostToClientCollector<Integer, String>(factory))
            .subscribe(new Action1<List<String>>() {
                @Override
                public void call(List<String> t1) {
                    System.out.println(t1);
                }
            });

        events.onNext(MembershipEvent.create(0, EventType.ADD));
        events.onNext(MembershipEvent.create(1, EventType.ADD));
        events.onNext(MembershipEvent.create(1, EventType.ADD));
        events.onNext(MembershipEvent.create(1, EventType.REMOVE));
        events.onNext(MembershipEvent.create(1, EventType.ADD));
        events.onNext(MembershipEvent.create(0, EventType.REMOVE));
        events.onNext(MembershipEvent.create(1, EventType.REMOVE));
    }
    
    @Test
    public void testReferenceCounting() {
        final List<String> removed = Lists.newArrayList();
        final List<String> created = Lists.newArrayList();
                
        HostToClientCachingLifecycleFactory<Integer, String> factory 
            = new HostToClientCachingLifecycleFactory<Integer, String>(
                new HostToClient<Integer, String>() {
                    public String call(Integer host) {
                        String client = "Client-" + host;
                        created.add(client);
                        return client;
                    }
                },
                FailureDetectingClientLifecycleFactory.<String>builder()
                    .withClientShutdown(new Action1<String>() {
                        @Override
                        public void call(String t1) {
                            removed.add(t1);
                        }
                    })
                    .build());

        PublishSubject<MembershipEvent<Integer>> events = PublishSubject.create();
        
        final AtomicReference<List<String>> lb1Clients = new AtomicReference<List<String>>();
        final AtomicReference<List<String>> lb2Clients = new AtomicReference<List<String>>();
        
        events
            .lift(new HostToClientCollector<Integer, String>(factory))
            .subscribe(new Action1<List<String>>() {
                @Override
                public void call(List<String> t1) {
                    lb1Clients.set(t1);
                }
            });
        
        events
            .lift(new HostToClientCollector<Integer, String>(factory))
            .subscribe(new Action1<List<String>>() {
                @Override
                public void call(List<String> t1) {
                    lb2Clients.set(t1);
                }
            });

        Assert.assertEquals(0,  created.size());
        events.onNext(MembershipEvent.create(0, EventType.ADD));
        Assert.assertEquals(Lists.<String>newArrayList("Client-0"), lb1Clients.get());
        Assert.assertEquals(Lists.<String>newArrayList("Client-0"), lb2Clients.get());
        Assert.assertEquals(0,  removed.size());
        events.onNext(MembershipEvent.create(0, EventType.REMOVE));
        Assert.assertEquals(Lists.<String>newArrayList(), lb1Clients.get());
        Assert.assertEquals(Lists.<String>newArrayList(), lb2Clients.get());
        Assert.assertEquals(1,  removed.size());
        events.onNext(MembershipEvent.create(0, EventType.ADD));
        Assert.assertEquals(Lists.<String>newArrayList("Client-0"), lb1Clients.get());
        Assert.assertEquals(Lists.<String>newArrayList("Client-0"), lb2Clients.get());
        Assert.assertEquals(1,  removed.size());
        events.onNext(MembershipEvent.create(0, EventType.REMOVE));
        Assert.assertEquals(Lists.<String>newArrayList(), lb1Clients.get());
        Assert.assertEquals(Lists.<String>newArrayList(), lb2Clients.get());
        Assert.assertEquals(2,  removed.size());
    }
}
