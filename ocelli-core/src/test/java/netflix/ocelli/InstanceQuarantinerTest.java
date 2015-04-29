package netflix.ocelli;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.InstanceQuarantiner.IncarnationFactory;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.TestScheduler;

public class InstanceQuarantinerTest {
     
    public static interface EventListener {
        public void onBegin();
        public void onSuccess();
        public void onFailed();
    }
    
    final static TestScheduler scheduler = new TestScheduler();
    
    public static class Client implements EventListener {
        
        public static Func1<Instance<Integer>, Instance<Client>> connector() {
            return new Func1<Instance<Integer>, Instance<Client>>() {
                @Override
                public Instance<Client> call(Instance<Integer> t1) {
                    return Instance.create(new Client(t1.getValue(), t1.getLifecycle()), t1.getLifecycle());
                }
            };
        }
        
        public static IncarnationFactory<Client> incarnationFactory() {
            return new IncarnationFactory<Client>() {
                @Override
                public Client create(
                        Client value,
                        InstanceEventListener listener,
                        Observable<Void> lifecycle) {
                    return new Client(value, listener, lifecycle);
                }
            };
        }
        
        private Integer address;
        private final Observable<Void> lifecycle;
        private final AtomicInteger counter;
        private AtomicInteger score = new AtomicInteger();
        private final InstanceEventListener listener;
        private final int id;
        
        public Client(Integer address, Observable<Void> lifecycle) {
            this.address = address;
            this.counter = new AtomicInteger();
            this.lifecycle = lifecycle;
            this.listener = null;
            
            id = 0;
        }
        
        Client(Client client, InstanceEventListener listener, Observable<Void> lifecycle) {
            this.address   = client.address;
            this.counter   = client.counter;
            this.lifecycle = lifecycle;
            this.listener  = listener;
            
            id = this.counter.incrementAndGet();
        }
        
        @Override
        public void onBegin() {
        }
        
        @Override
        public void onSuccess() {
            listener.onEvent(InstanceEvent.EXECUTION_SUCCESS, 0, TimeUnit.SECONDS, null, null);
        }
        
        @Override
        public void onFailed() {
            listener.onEvent(InstanceEvent.EXECUTION_FAILED, 0, TimeUnit.SECONDS, new Exception("Failed"), null);
        }
        
        public Integer getValue() {
            return address;
        }
        
        public int getId() {
            return id;
        }
        
        public String toString() {
            return address.toString() + "[" + id + "]";
        }
        
        public static Func2<Client, Client, Client> compareByMetric() {
            return new Func2<Client, Client, Client>() {
                @Override
                public Client call(Client t1, Client t2) {
                    int v1 = t1.score.get();
                    int v2 = t2.score.get();
                    return v1 > v2 ? t1 : t2;
                }
            };
        }

        public Observable<Void> getLifecycle() {
            return lifecycle;
        }
    }
    
    @Test
    public void basicTest() {
        final InstanceSubject<Integer> instances = InstanceSubject.create();
        
        final LoadBalancer<Client> lb = LoadBalancer
                .fromSource(instances.map(Client.connector()))
                .withQuarantiner(Client.incarnationFactory(), Delays.fixed(1, TimeUnit.SECONDS), scheduler)
                .build(RoundRobinLoadBalancer.<Client>create());

        instances.add(1);
        
        // Load balancer now has one instance
        Client c = lb.toBlocking().first();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(1, c.getId());
        
        // Force the instance to fail
        c.onFailed();
        
        // Load balancer is now empty
        try {
            c = lb.toBlocking().first();
            Assert.fail("Load balancer should be empty");
        }
        catch (NoSuchElementException e) {
        }
        
        // Advance past quarantine time
        scheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        c = lb.toBlocking().first();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(2, c.getId());
        
        // Force the instance to fail
        c.onFailed();
        
        // Load balancer is now empty
        try {
            c = lb.toBlocking().first();
            Assert.fail("Load balancer should be empty");
        }
        catch (NoSuchElementException e) {
        }
        
        // Advance past quarantine time
        scheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        c = lb.toBlocking().first();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(3, c.counter.get());
        
        // Remove the instance entirely
        instances.remove(1);
        try {
            c = lb.toBlocking().first();
            Assert.fail();
        }
        catch (NoSuchElementException e) {
        }
        
        System.out.println(c);
    }
    
    @Test
    @Ignore
    public void test() {
        final InstanceSubject<Integer> instances = InstanceSubject.create();
        
        final LoadBalancer<Client> lb = LoadBalancer
                .fromSource(instances.map(Client.connector()))
                .withQuarantiner(Client.incarnationFactory(), Delays.fixed(1, TimeUnit.SECONDS), scheduler)
                .build(RoundRobinLoadBalancer.<Client>create());
        
        // Add to the load balancer
        instances.add(1);
        instances.add(2);
        
        // Perform 10 operations
        List<String> result = Observable
            .interval(100, TimeUnit.MILLISECONDS)
            .concatMap(new Func1<Long, Observable<String>>() {
                @Override
                public Observable<String> call(final Long counter) {
                    return Observable.just(lb.toBlocking().first())
                        .concatMap(new Func1<Client, Observable<String>>() {
                            @Override
                            public Observable<String> call(Client instance) {
                                instance.onBegin();
                                if (1 == instance.getValue()) {
                                    instance.onFailed();
                                    return Observable.error(new Exception("Failed"));
                                }
                                instance.onSuccess();
                                return Observable.just(instance + "-" + counter);
                            }
                        })
                        .retry(1);
                }
            })
            .take(10)
            .toList()
            .toBlocking()
            .first()
            ;
    }
    
    @Test
    public void integrationTest() {
        final InstanceSubject<Integer> instances = InstanceSubject.create();
        
        final LoadBalancer<Client> lb = LoadBalancer
                .fromSource(instances.map(Client.connector()))
                .withQuarantiner(Client.incarnationFactory(), Delays.fixed(1, TimeUnit.SECONDS), scheduler)
                .build(ChoiceOfTwoLoadBalancer.<Client>create(Client.compareByMetric()));
        
        instances.add(1);
        Client client = lb.toBlocking().first();
        
        instances.add(2);
        client = lb.toBlocking().first();
        
    }
}
