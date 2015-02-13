package netflix.ocelli.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import netflix.ocelli.FailureDetectingInstanceFactory;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.MutableInstance;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.ManualFailureDetector;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TestClientConnectorFactory;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Metrics;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observer;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

public class BackupExecutorTest {
    private static final Logger LOG = LoggerFactory.getLogger(BackupExecutorTest.class);
    
    private LoadBalancer<TestClient> lb;
    private PublishSubject<Instance<TestClient>> hosts = PublishSubject.create();
    private TestClientConnectorFactory clientConnector = new TestClientConnectorFactory();
    private ManualFailureDetector failureDetector = new ManualFailureDetector();
    private BackupExecutor<TestClient, String, String> executor;
    
    private final static long BACKUP_REQUEST_TIMEOUT = 10;
    private final static long HALF_BACKUP_REQUEST_TIMEOUT = BACKUP_REQUEST_TIMEOUT/2;
    
    private final TestScheduler scheduler = Schedulers.test();
    
    private final Instance<TestClient> fastResponse1 = MutableInstance.from(TestClient.create("host-1", Behaviors.delay(1, TimeUnit.MILLISECONDS, scheduler)));
    private final Instance<TestClient> fastResponse2 = MutableInstance.from(TestClient.create("host-2", Behaviors.delay(1, TimeUnit.MILLISECONDS, scheduler)));
    
    private final Instance<TestClient> emptyResponse1 = MutableInstance.from(TestClient.create("host-1", Behaviors.empty()));
    private final Instance<TestClient> emptyResponse2 = MutableInstance.from(TestClient.create("host-2", Behaviors.empty()));
    
    private final Instance<TestClient> slowResponse1 = MutableInstance.from(TestClient.create("host-1", Behaviors.delay(BACKUP_REQUEST_TIMEOUT * 3, TimeUnit.MILLISECONDS, scheduler)));
    private final Instance<TestClient> slowResponse2 = MutableInstance.from(TestClient.create("host-2", Behaviors.delay(BACKUP_REQUEST_TIMEOUT * 3, TimeUnit.MILLISECONDS, scheduler)));
    
    private final Instance<TestClient> delayedResponse1 = MutableInstance.from(TestClient.create("host-1", Behaviors.delay(BACKUP_REQUEST_TIMEOUT + 2, TimeUnit.MILLISECONDS, scheduler)));
    private final Instance<TestClient> delayedResponse2 = MutableInstance.from(TestClient.create("host-2", Behaviors.delay(BACKUP_REQUEST_TIMEOUT + 2, TimeUnit.MILLISECONDS, scheduler)));
    
    private final Instance<TestClient> fastError1 = MutableInstance.from(TestClient.create("host-1", Behaviors.failure(1, TimeUnit.MILLISECONDS, scheduler)));
    private final Instance<TestClient> fastError2 = MutableInstance.from(TestClient.create("host-2", Behaviors.failure(1, TimeUnit.MILLISECONDS, scheduler)));
    
    private final Instance<TestClient> delayedError1 = MutableInstance.from(TestClient.create("host-1", Behaviors.failure(BACKUP_REQUEST_TIMEOUT + 1, TimeUnit.MILLISECONDS, scheduler)));
    private final Instance<TestClient> delayedError2 = MutableInstance.from(TestClient.create("host-2", Behaviors.failure(BACKUP_REQUEST_TIMEOUT + 1, TimeUnit.MILLISECONDS, scheduler)));
    
    @Rule
    public TestName testName = new TestName();
    private Func1<TestClient, Observable<String>> op;
    
    @BeforeClass
    public static void setup() {
    }
    
    @Before
    public void before() {
        final FailureDetectingInstanceFactory<TestClient> factory =
                FailureDetectingInstanceFactory.<TestClient>builder()
                .withQuarantineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
                .withFailureDetector(failureDetector)
                .withClientConnector(clientConnector)
                .build();
    
        this.lb = RoundRobinLoadBalancer.from(
                hosts.map(new Func1<Instance<TestClient>, Instance<TestClient>>() {
                        @Override
                        public Instance<TestClient> call(Instance<TestClient> t1) {
                            return factory.call(t1.getValue());
                        }
                     })
//                     .map(TestClient.memberToInstance(factory))  
                     .compose(new InstanceCollector<TestClient>()));  
        
        this.executor = BackupExecutor.<TestClient, String, String>builder(lb)
                .withTimeoutMetric(Metrics.memoize(BACKUP_REQUEST_TIMEOUT))
                .withScheduler(scheduler)
                .withClientFunc(TestClient.func())
                .build();
        
        this.op = new Func1<TestClient, Observable<String>>() {
            @Override
            public Observable<String> call(TestClient client) {
                return client.execute(new Func1<TestClient, Observable<String>>() {
                    @Override
                    public Observable<String> call(TestClient t1) {
                        LOG.info("Operation on " + t1);
                        return Observable.just(t1.id());
                    }
                });
            }
        };
    }
    
    @After
    public void afterTest() {
        if (this.lb != null) {
            this.lb.shutdown();
        }
    }
    
    public static class TestObserver<T> implements Observer<T> {
        private List<T> items = new ArrayList<T>();
        private volatile Throwable e;
        private boolean completed = false;
        
        @Override
        public void onCompleted() {
            completed = true;
        }

        @Override
        public void onError(Throwable e) {
            this.e = e;
            this.completed = true;
        }

        @Override
        public void onNext(T t) {
            items.add(t);
        }
        
        public boolean isCompleted() {
            return completed;
        }
        
        public int size() { 
            return items.size();
        }
        
        public T first() {
            if (items.isEmpty()) {
                return null;
            }
            return items.get(0);
        }

        public boolean hasError() {
            return e != null;
        }
        
        public Throwable getError() {
            return e;
        }
    }
    
    @Test
    public void firstRespondsBeforeBackupTimeout() throws InterruptedException {
        hosts.onNext(fastResponse1);
        hosts.onNext(fastResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.call(testName.getMethodName()).subscribe(response);
        
        scheduler.advanceTimeBy(HALF_BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        
        Assert.assertEquals("host-1", response.first());
        
        Assert.assertEquals(1, fastResponse1.getValue().getExecuteCount());
        Assert.assertEquals(1, fastResponse1.getValue().getOnSubscribeCount());
        Assert.assertEquals(0, fastResponse1.getValue().getOnErrorCount());
        Assert.assertEquals(1, fastResponse1.getValue().getOnNextCount());
        Assert.assertEquals(1, fastResponse1.getValue().getOnCompletedCount());
        
        Assert.assertEquals(0, fastResponse2.getValue().getExecuteCount());
        Assert.assertEquals(0, fastResponse2.getValue().getOnSubscribeCount());
        Assert.assertEquals(0, fastResponse2.getValue().getOnErrorCount());
        Assert.assertEquals(0, fastResponse2.getValue().getOnNextCount());
        Assert.assertEquals(0, fastResponse2.getValue().getOnCompletedCount());
    }
    
    @Test
    public void firstRespondsAfterBackupTimeout() throws InterruptedException {
        hosts.onNext(delayedResponse1);
        hosts.onNext(delayedResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.call(testName.getMethodName()).subscribe(response);
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, fastResponse1.getValue().getOnNextCount());
        scheduler.advanceTimeBy(2, TimeUnit.MILLISECONDS);
        
        Assert.assertEquals("host-1", response.first());
        
        Assert.assertEquals(1, delayedResponse1.getValue().getExecuteCount());
        Assert.assertEquals(1, delayedResponse1.getValue().getOnSubscribeCount());
        Assert.assertEquals(0, delayedResponse1.getValue().getOnErrorCount());
        Assert.assertEquals(1, delayedResponse1.getValue().getOnNextCount());
        Assert.assertEquals(1, delayedResponse1.getValue().getOnCompletedCount());
        
        Assert.assertEquals(1, delayedResponse2.getValue().getExecuteCount());
        Assert.assertEquals(1, delayedResponse2.getValue().getOnSubscribeCount());
        Assert.assertEquals(0, delayedResponse2.getValue().getOnErrorCount());
        Assert.assertEquals(0, delayedResponse2.getValue().getOnNextCount());
        Assert.assertEquals(0, delayedResponse2.getValue().getOnCompletedCount());        
    }

    @Test
    public void secondRespondsAfterBackupTimeout() throws InterruptedException {
        hosts.onNext(slowResponse1);
        hosts.onNext(fastResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.call(testName.getMethodName()).subscribe(response);
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, fastResponse1.getValue().getOnNextCount());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        
        Assert.assertEquals("host-2", response.first());
        
        Assert.assertEquals(1, slowResponse1.getValue().getExecuteCount());
        Assert.assertEquals(1, slowResponse1.getValue().getOnSubscribeCount());
        Assert.assertEquals(0, slowResponse1.getValue().getOnErrorCount());
        Assert.assertEquals(0, slowResponse1.getValue().getOnNextCount());
        Assert.assertEquals(0, slowResponse1.getValue().getOnCompletedCount());
        
        Assert.assertEquals(1, fastResponse2.getValue().getExecuteCount());
        Assert.assertEquals(1, fastResponse2.getValue().getOnSubscribeCount());
        Assert.assertEquals(0, fastResponse2.getValue().getOnErrorCount());
        Assert.assertEquals(1, fastResponse2.getValue().getOnNextCount());
        Assert.assertEquals(1, fastResponse2.getValue().getOnCompletedCount());        
    }

    @Test
    public void secondRespondsAfterFailedFirst() throws InterruptedException {
        hosts.onNext(fastError1);
        hosts.onNext(fastResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.call(testName.getMethodName()).subscribe(response);
        
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, fastResponse1.getValue().getOnNextCount());
        
        Assert.assertTrue(fastError1.getValue().hasError());
        Assert.assertFalse(response.hasError());
        
        Assert.assertEquals(1, fastError1.getValue().getExecuteCount());
        Assert.assertEquals(1, fastError1.getValue().getOnSubscribeCount());
        Assert.assertEquals(1, fastError1.getValue().getOnErrorCount());
        Assert.assertEquals(0, fastError1.getValue().getOnNextCount());
        Assert.assertEquals(0, fastError1.getValue().getOnCompletedCount());
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, fastResponse2.getValue().getExecuteCount());
        Assert.assertEquals(1, fastResponse2.getValue().getOnSubscribeCount());
        Assert.assertEquals(0, fastResponse2.getValue().getOnErrorCount());
        Assert.assertEquals(1, fastResponse2.getValue().getOnNextCount());
        Assert.assertEquals(1, fastResponse2.getValue().getOnCompletedCount());        
    }
    
    @Test
    public void bothFail() throws InterruptedException {
        hosts.onNext(fastError1);
        hosts.onNext(fastError2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.call(testName.getMethodName()).subscribe(response);
        
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, fastResponse1.getValue().getOnNextCount());
        
        Assert.assertTrue(fastError1.getValue().hasError());
        Assert.assertFalse(response.hasError());
        
        Assert.assertEquals(1, fastError1.getValue().getExecuteCount());
        Assert.assertEquals(1, fastError1.getValue().getOnSubscribeCount());
        Assert.assertEquals(1, fastError1.getValue().getOnErrorCount());
        Assert.assertEquals(0, fastError1.getValue().getOnNextCount());
        Assert.assertEquals(0, fastError1.getValue().getOnCompletedCount());
        Assert.assertFalse(response.hasError());
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, fastError2.getValue().getExecuteCount());
        Assert.assertEquals(1, fastError2.getValue().getOnSubscribeCount());
        Assert.assertEquals(1, fastError2.getValue().getOnErrorCount());
        Assert.assertEquals(0, fastError2.getValue().getOnNextCount());
        Assert.assertEquals(0, fastError2.getValue().getOnCompletedCount());        
        
        Assert.assertTrue(response.hasError());
        
    }
    
    @Test
    public void secondRespondsAfterFirstEmpty() {
        hosts.onNext(emptyResponse1);
        hosts.onNext(fastResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.call(testName.getMethodName()).subscribe(response);
        
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, emptyResponse1.getValue().getOnNextCount());
        Assert.assertEquals(1, emptyResponse1.getValue().getOnCompletedCount());
        
        Assert.assertEquals(0, fastResponse2.getValue().getOnUnSubscribeCount());
        Assert.assertFalse(response.isCompleted());
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT+1, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, fastResponse2.getValue().getOnSubscribeCount());
        Assert.assertEquals(1, fastResponse2.getValue().getExecuteCount());
        Assert.assertEquals(1, fastResponse2.getValue().getOnNextCount());
        
        Assert.assertEquals(response.first(), "host-2");
    }
    
    @Test
    public void firstRespondsAfterSecondEmpty() {
        hosts.onNext(slowResponse1);
        hosts.onNext(emptyResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.call(testName.getMethodName()).subscribe(response);
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT*3, TimeUnit.MILLISECONDS);
        
        Assert.assertEquals(1, emptyResponse2.getValue().getOnSubscribeCount());
        Assert.assertEquals(0, emptyResponse2.getValue().getOnNextCount());
        
        Assert.assertEquals(response.first(), "host-1");
    }
    
    @Test
    public void errorAfterBothEmpty() {
        hosts.onNext(emptyResponse1);
        hosts.onNext(emptyResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.call(testName.getMethodName()).subscribe(response);
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT+1, TimeUnit.MILLISECONDS);
        
        Assert.assertTrue(response.hasError());
    }
}
