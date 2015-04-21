package netflix.ocelli;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import netflix.ocelli.util.RxUtil;

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
    
    public static class Client implements EventListener, Instance<Client> {
        
        public static Func1<Instance<Integer>, Client> factory() {
            return new Func1<Instance<Integer>, Client>() {
                @Override
                public Client call(Instance<Integer> t1) {
                    return new Client(t1.getValue(), t1.getLifecycle());
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
        
        public static Func1<Instance<Client>, Observable<Instance<Client>>> failureDetector() {
            return new Func1<Instance<Client>, Observable<Instance<Client>>>() {
                @Override
                public Observable<Instance<Client>> call(Instance<Client> i) {
                    i = new Client(i.getValue());
                    Observable<Instance<Client>> o = Observable.just(i);

                    long delay = i.getValue().counter.get();
                    if (delay > 0) {
                        o = o.delaySubscription(i.getValue().counter.get(), TimeUnit.SECONDS, scheduler);
                    }
                    
                    return o;
                }
            };
        }
        
        private Integer address;
        private final BehaviorSubject<Void> control = BehaviorSubject.create();
        private final Observable<Void> lifecycle;
        private AtomicLong counter = new AtomicLong();
        private AtomicInteger score = new AtomicInteger();

        public Client(Integer address, Observable<Void> lifecycle) {
            this.address = address;
            this.counter = new AtomicLong();
            this.lifecycle = lifecycle;
        }
        
        Client(Client client) {
            this.address   = client.address;
            this.counter   = client.counter;
            this.lifecycle = client.lifecycle.ambWith(control).cache();
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
            System.out.println("onFailed");
            counter.incrementAndGet();
            control.onCompleted();
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

        @Override
        public Observable<Void> getLifecycle() {
            return lifecycle;
        }

        @Override
        public Client getValue() {
            return this;
        }
    }
    
    @Test
    public void basicTest() {
        final InstanceSubject<Integer> instances = InstanceSubject.create();
        final RoundRobinLoadBalancer<Client> lb = RoundRobinLoadBalancer.create();
        
        instances
            // Convert instances from address 'Integer' to implementation 'Instance'
            .map(Client.factory())
            .doOnNext(RxUtil.info("Primary"))
            // Reconnect logic
            .flatMap(InstanceQuarantiner.create(Client.failureDetector()))
            .doOnNext(RxUtil.info("Secondary"))
            // Aggregate into a List
            .compose(InstanceCollector.<Client>create())
            .doOnNext(RxUtil.info("Pool"))
            // Forward to the load balancer
            .subscribe(lb);

        instances.add(1);
        
        // Load balancer now has one instance
        Client c = lb.next();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(0, c.counter.get());
        
        // Force the instance to fail
        c.onFailed();
        
        // Load balancer is now empty
        try {
            c = lb.next();
            Assert.fail("Load balancer should be empty");
        }
        catch (NoSuchElementException e) {
        }
        
        // Advance past quarantine time
        scheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        c = lb.next();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(1, c.counter.get());
        
        // Force the instance to fail
        c.onFailed();
        
        // Load balancer is now empty
        try {
            c = lb.next();
            Assert.assertEquals(2, c.counter.get());
            Assert.fail("Load balancer should be empty");
        }
        catch (NoSuchElementException e) {
        }
        
        // Advance past quarantine time
        scheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        c = lb.next();
        Assert.assertNotNull("Load balancer should have an active intance", c);
        Assert.assertEquals(2, c.counter.get());
        
        // Remove the instance entirely
        instances.remove(1);
        try {
            c = lb.next();
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
            .map(Client.factory())
            // Reconnect logic
            .flatMap(InstanceQuarantiner.create(Client.failureDetector()))
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
            .map(Client.factory())
            // Reconnect logic
            .flatMap(InstanceQuarantiner.create(Client.failureDetector()))
            // Aggregate into a List
            .compose(InstanceCollector.<Client>create())
            // Forward to the load balancer
            .subscribe(lb);

        instances.add(1);
        Client client = lb.next();
        
        instances.add(2);
        client = lb.next();
        
    }
}
