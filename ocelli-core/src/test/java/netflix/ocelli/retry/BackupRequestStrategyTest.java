package netflix.ocelli.retry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.functions.Metrics;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import netflix.ocelli.retrys.BackupRequestRetryStrategy;
import netflix.ocelli.util.RxUtil;

import org.junit.Test;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.TestScheduler;

import com.google.common.collect.Lists;

public class BackupRequestStrategyTest {
    
    private Func1<Observable<Integer>, Observable<String>> Operation = new Func1<Observable<Integer>, Observable<String>>() {
        @Override
        public Observable<String> call(Observable<Integer> t1) {
            return t1.map(new Func1<Integer, String>() {
                @Override
                public String call(Integer t1) {
                    return t1.toString();
                }
            });
        }
    };
    
    private TestScheduler scheduler = new TestScheduler();
    private BackupRequestRetryStrategy<String> strategy = BackupRequestRetryStrategy.<String>builder()
            .withScheduler(scheduler)
            .withTimeoutMetric(Metrics.memoize(1000L))
            .build();
    
    @Test
    public void firstSucceedsFast() {
        LoadBalancer<Observable<Integer>> lb = RoundRobinLoadBalancer.create();
        
        lb.call(Lists.newArrayList(
                Observable.just(1),
                Observable.just(2),
                Observable.just(3)));
        
        final AtomicInteger lbCounter = new AtomicInteger();
        final AtomicReference<String> result = new AtomicReference<String>();
        
        lb
            .doOnNext(RxUtil.increment(lbCounter))
            .flatMap(Operation)
            .compose(strategy)
            .doOnNext(RxUtil.set(result))
            .subscribe();

        scheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        
        Assert.assertEquals("1", result.get());
        Assert.assertEquals(1, lbCounter.get());
    }
    
    @Test
    public void firstNeverSecondSucceeds() {
        LoadBalancer<Observable<Integer>> lb = RoundRobinLoadBalancer.create();
        
        lb.call(Lists.newArrayList(
                Observable.<Integer>never(),
                Observable.just(2),
                Observable.just(3)));
        
        final AtomicInteger lbCounter = new AtomicInteger();
        final AtomicReference<String> result = new AtomicReference<String>();
        
        lb
            .doOnNext(RxUtil.increment(lbCounter))
            .flatMap(Operation)
            .compose(strategy)
            .doOnNext(RxUtil.set(result))
            .subscribe();

        scheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        
        Assert.assertEquals("2", result.get());
        Assert.assertEquals(2, lbCounter.get());
    }
    
    @Test
    public void firstFailsSecondSucceeds() {
        LoadBalancer<Observable<Integer>> lb = RoundRobinLoadBalancer.create();
        
        lb.call(Lists.newArrayList(
                Observable.<Integer>error(new Exception("1")),
                Observable.just(2),
                Observable.just(3)));
        
        final AtomicInteger lbCounter = new AtomicInteger();
        final AtomicReference<String> result = new AtomicReference<String>();
        
        lb
            .doOnNext(RxUtil.increment(lbCounter))
            .flatMap(Operation)
            .compose(strategy)
            .doOnNext(RxUtil.set(result))
            .subscribe();

        scheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        
        Assert.assertEquals("2", result.get());
        Assert.assertEquals(2, lbCounter.get());
    }
    
    @Test
    public void bothDelayed() {
        LoadBalancer<Observable<Integer>> lb = RoundRobinLoadBalancer.create();
        
        lb.call(Lists.newArrayList(
                Observable.just(1).delaySubscription(2, TimeUnit.SECONDS, scheduler),
                Observable.just(2).delaySubscription(2, TimeUnit.SECONDS, scheduler),
                Observable.just(3)));
        
        final AtomicInteger lbCounter = new AtomicInteger();
        final AtomicReference<String> result = new AtomicReference<String>();
        
        lb
            .doOnNext(RxUtil.increment(lbCounter))
            .flatMap(Operation)
            .compose(strategy)
            .doOnNext(RxUtil.set(result))
            .subscribe();

        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(3, TimeUnit.SECONDS);
        
        Assert.assertEquals(2, lbCounter.get());
        Assert.assertEquals("1", result.get());
    }
    
    @Test
    public void bothFailed() {
        LoadBalancer<Observable<Integer>> lb = RoundRobinLoadBalancer.create();
        
        lb.call(Lists.newArrayList(
                Observable.<Integer>error(new Exception("1")),
                Observable.<Integer>error(new Exception("2")),
                Observable.just(3)));
        
        final AtomicInteger lbCounter = new AtomicInteger();
        final AtomicReference<String> result = new AtomicReference<String>();
        final AtomicBoolean failed = new AtomicBoolean(false);
        
        lb
            .doOnNext(RxUtil.increment(lbCounter))
            .flatMap(Operation)
            .compose(strategy)
            .doOnNext(RxUtil.set(result))
            .doOnError(new Action1<Throwable>() {
                @Override
                public void call(Throwable t1) {
                    failed.set(true);
                }
            })
            .subscribe();

        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(3, TimeUnit.SECONDS);
        
        Assert.assertEquals(2, lbCounter.get());
        Assert.assertTrue(failed.get());
    }
    
    @Test
    public void firstSucceedsSecondFailsAfterBackupStarted() {
        LoadBalancer<Observable<Integer>> lb = RoundRobinLoadBalancer.create();
        
        lb.call(Lists.newArrayList(
                Observable.just(1).delaySubscription(2, TimeUnit.SECONDS, scheduler),
                Observable.<Integer>error(new Exception("2")),
                Observable.just(3)));
        
        final AtomicInteger lbCounter = new AtomicInteger();
        final AtomicReference<String> result = new AtomicReference<String>();
        
        lb
            .doOnNext(RxUtil.increment(lbCounter))
            .flatMap(Operation)
            .compose(strategy)
            .doOnNext(RxUtil.set(result))
            .subscribe();

        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        
        Assert.assertEquals("1", result.get());
        Assert.assertEquals(2, lbCounter.get());
    }
}
