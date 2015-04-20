package netflix.ocelli;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.TestScheduler;
import rx.subjects.BehaviorSubject;

public class InstanceQuarantinerTest {
     
    public static interface EventListener {
        public void onBegin();
        public void onSuccess();
        public void onFailed();
    }
    
    final static TestScheduler scheduler = new TestScheduler();
    
    public static class Client implements EventListener{
        
        public static Func1<Integer, Client> factory() {
            return new Func1<Integer, Client>() {
                @Override
                public Client call(Integer t1) {
                    return new Client(t1);
                }
            };
        }
        
        public static Func1<Client, Long> fixed(final long amount) {
            return new Func1<Client, Long>() {
                @Override
                public Long call(Client t1) {
                    return amount * t1.getFailedCount();
                }
            };
        }
        
        public static Func1<Client, Observable<Instance<Client>>> failureDetector() {
            return new Func1<Client, Observable<Instance<Client>>>() {
                @Override
                public Observable<Instance<Client>> call(Client t1) {
                    t1 = t1.clone();
                    Observable<Instance<Client>> o = Observable
                            .just(Instance.from(t1, t1.lifecycle));

                    long delay = t1.counter.get();
                    if (delay > 0) {
                        o = o.delaySubscription(t1.counter.get(), TimeUnit.SECONDS, scheduler);
                    }
                    
                    return o;
                }
            };
        }
        
        private Integer address;
        private BehaviorSubject<Void> lifecycle = BehaviorSubject.create();
        private AtomicLong counter = new AtomicLong();
        private AtomicInteger score = new AtomicInteger();

        public Client(Integer address) {
            this.address = address;
            this.counter = new AtomicLong();
            System.out.println("Create client " + address);
        }
        
        public Client(Client client) {
            this.address = client.address;
            this.counter = client.counter;
        }
        
        @Override
        public void onBegin() {
            System.out.println("begin " + address);
        }
        
        @Override
        public void onSuccess() {
            counter.set(0);
            System.out.println("success " + address);
        }
        
        @Override
        public void onFailed() {
            counter.incrementAndGet();
            lifecycle.onCompleted();
        }
        
        public Integer getAddress() {
            return address;
        }
        
        public long getFailedCount() {
            return counter.get();
        }
        
        public String toString() {
            return address.toString();
        }
        
        public Client clone() {
            return new Client(this);
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
    }
    
    @Test
    public void basicTest() {
        final InstanceSubject<Integer> instances = InstanceSubject.create();
        final RoundRobinLoadBalancer<Client> lb = RoundRobinLoadBalancer.create();
        
        instances
            // Convert instances from address 'Integer' to implementation 'Instance'
            .map(Instance.transform(Client.factory()))
            // Reconnect logic
            .flatMap(InstanceQuarantiner.<Client>create(Client.failureDetector()))
            // Aggregate into a List
            .compose(InstanceCollector.<Client>create())
            // Forward to the load balancer
            .subscribe(lb);

        instances.add(1);
        
        // Load balancer now has one instance
        Client c = lb.toObservable().toBlocking().first();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(0, c.counter.get());
        
        // Force the instance to fail
        c.onFailed();
        
        // Load balancer is now empty
        try {
            c = lb.toObservable().toBlocking().first();
            Assert.fail("Load balancer should be empty");
        }
        catch (NoSuchElementException e) {
        }
        
        // Advance past quarantine time
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        c = lb.toObservable().toBlocking().first();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(1, c.counter.get());
        
        // Force the instance to fail
        c.onFailed();
        
        // Load balancer is now empty
        try {
            c = lb.toObservable().toBlocking().first();
            Assert.assertEquals(2, c.counter.get());
            Assert.fail("Load balancer should be empty");
        }
        catch (NoSuchElementException e) {
        }
        
        // Advance past quarantine time
        scheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        c = lb.toObservable().toBlocking().first();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(2, c.counter.get());
        
        // Remove the instance entirely
        instances.remove(1);
        try {
            c = lb.toObservable().toBlocking().first();
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
        
        final RoundRobinLoadBalancer<Client> lb = RoundRobinLoadBalancer.create();
        
        instances
            // Convert instances from address 'Integer' to implementation 'Instance'
            .map(Instance.transform(Client.factory()))
            // Reconnect logic
            .flatMap(InstanceQuarantiner.<Client>create(Client.failureDetector()))
            // Aggregate into a List
            .compose(InstanceCollector.<Client>create())
            // Forward to the load balancer
            .subscribe(lb);
        
        // Add to the load balancer
        instances.add(1);
        instances.add(2);
        
        // Perform 10 operations
        List<String> result = Observable
            .interval(100, TimeUnit.MILLISECONDS)
            .concatMap(new Func1<Long, Observable<String>>() {
                @Override
                public Observable<String> call(final Long counter) {
                    return lb
                        .toObservable()
                        .concatMap(new Func1<Client, Observable<String>>() {
                            @Override
                            public Observable<String> call(Client instance) {
                                instance.onBegin();
                                if (1 == instance.getAddress()) {
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
        final ChoiceOfTwoLoadBalancer<Client> lb = ChoiceOfTwoLoadBalancer.create(Client.compareByMetric());
        
        instances
            // Convert from address 'Integer' to implementation 'Instance'
            .map(Instance.transform(Client.factory()))
            // Reconnect logic
            .flatMap(InstanceQuarantiner.<Client>create(Client.failureDetector()))
            // Aggregate into a List
            .compose(InstanceCollector.<Client>create())
            // Forward to the load balancer
            .subscribe(lb);

        instances.add(1);
        Client client = lb.toObservable().toBlocking().first();
        
        instances.add(2);
        client = lb.toObservable().toBlocking().first();
        
    }
}
