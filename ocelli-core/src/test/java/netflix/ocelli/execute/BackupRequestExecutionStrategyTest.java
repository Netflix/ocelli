package netflix.ocelli.execute;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import netflix.ocelli.ClientCollector;
import netflix.ocelli.ClientLifecycleFactory;
import netflix.ocelli.FailureDetectingClientLifecycleFactory;
import netflix.ocelli.LoadBalancer;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.client.Behaviors;
import netflix.ocelli.client.ManualFailureDetector;
import netflix.ocelli.client.TestClient;
import netflix.ocelli.client.TestClientConnectorFactory;
import netflix.ocelli.functions.Delays;
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
import rx.functions.Func0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

public class BackupRequestExecutionStrategyTest {
    private static final Logger LOG = LoggerFactory.getLogger(BackupRequestExecutionStrategyTest.class);
    
    private LoadBalancer<TestClient> lb;
    private PublishSubject<TestClient> hosts = PublishSubject.create();
    private TestClientConnectorFactory clientConnector = new TestClientConnectorFactory();
    private ManualFailureDetector failureDetector = new ManualFailureDetector();
    private BackupRequestExecutionStrategy<TestClient> executor;
    
    private final static int BACKUP_REQUEST_TIMEOUT = 10;
    private final static int HALF_BACKUP_REQUEST_TIMEOUT = BACKUP_REQUEST_TIMEOUT/2;
    
    private final TestScheduler scheduler = Schedulers.test();
    
    private final TestClient fastResponse1 = TestClient.create("host-1", Behaviors.delay(1, TimeUnit.MILLISECONDS, scheduler));
    private final TestClient fastResponse2 = TestClient.create("host-2", Behaviors.delay(1, TimeUnit.MILLISECONDS, scheduler));
    
    private final TestClient emptyResponse1 = TestClient.create("host-1", Behaviors.empty());
    private final TestClient emptyResponse2 = TestClient.create("host-2", Behaviors.empty());
    
    private final TestClient slowResponse1 = TestClient.create("host-1", Behaviors.delay(BACKUP_REQUEST_TIMEOUT * 3, TimeUnit.MILLISECONDS, scheduler));
    private final TestClient slowResponse2 = TestClient.create("host-2", Behaviors.delay(BACKUP_REQUEST_TIMEOUT * 3, TimeUnit.MILLISECONDS, scheduler));
    
    private final TestClient delayedResponse1 = TestClient.create("host-1", Behaviors.delay(BACKUP_REQUEST_TIMEOUT + 2, TimeUnit.MILLISECONDS, scheduler));
    private final TestClient delayedResponse2 = TestClient.create("host-2", Behaviors.delay(BACKUP_REQUEST_TIMEOUT + 2, TimeUnit.MILLISECONDS, scheduler));
    
    private final TestClient fastError1 = TestClient.create("host-1", Behaviors.failure(1, TimeUnit.MILLISECONDS, scheduler));
    private final TestClient fastError2 = TestClient.create("host-2", Behaviors.failure(1, TimeUnit.MILLISECONDS, scheduler));
    
    private final TestClient delayedError1 = TestClient.create("host-1", Behaviors.failure(BACKUP_REQUEST_TIMEOUT + 1, TimeUnit.MILLISECONDS, scheduler));
    private final TestClient delayedError2 = TestClient.create("host-2", Behaviors.failure(BACKUP_REQUEST_TIMEOUT + 1, TimeUnit.MILLISECONDS, scheduler));
    
    @Rule
    public TestName testName = new TestName();
    private Func1<TestClient, Observable<String>> op;
    
    @BeforeClass
    public static void setup() {
    }
    
    @Before
    public void before() {
        ClientLifecycleFactory<TestClient> factory =
                FailureDetectingClientLifecycleFactory.<TestClient>builder()
                .withQuarantineStrategy(Delays.fixed(1, TimeUnit.SECONDS))
                .withFailureDetector(failureDetector)
                .withClientConnector(clientConnector)
                .build();
    
        this.lb = RoundRobinLoadBalancer.create(
                hosts.map(MembershipEvent.<TestClient>toEvent(EventType.ADD)).lift(ClientCollector.create(factory)));  
        
        this.executor = BackupRequestExecutionStrategy.builder(lb)
                .withBackupTimeout(
                    new Func0<Integer>() {
                        @Override
                        public Integer call() {
                            return BACKUP_REQUEST_TIMEOUT;
                        }
                    },
                    TimeUnit.MILLISECONDS)
                .withScheduler(scheduler)
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
    }
    
    @Test
    public void firstRespondsBeforeBackupTimeout() throws InterruptedException {
        hosts.onNext(fastResponse1);
        hosts.onNext(fastResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.execute(this.op).subscribe(response);
        
        scheduler.advanceTimeBy(HALF_BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        
        Assert.assertEquals("host-1", response.first());
        
        Assert.assertEquals(1, fastResponse1.getExecuteCount());
        Assert.assertEquals(1, fastResponse1.getOnSubscribeCount());
        Assert.assertEquals(0, fastResponse1.getOnErrorCount());
        Assert.assertEquals(1, fastResponse1.getOnNextCount());
        Assert.assertEquals(1, fastResponse1.getOnCompletedCount());
        
        Assert.assertEquals(0, fastResponse2.getExecuteCount());
        Assert.assertEquals(0, fastResponse2.getOnSubscribeCount());
        Assert.assertEquals(0, fastResponse2.getOnErrorCount());
        Assert.assertEquals(0, fastResponse2.getOnNextCount());
        Assert.assertEquals(0, fastResponse2.getOnCompletedCount());
    }
    
    @Test
    public void firstRespondsAfterBackupTimeout() throws InterruptedException {
        hosts.onNext(delayedResponse1);
        hosts.onNext(delayedResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.execute(this.op).subscribe(response);
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, fastResponse1.getOnNextCount());
        scheduler.advanceTimeBy(2, TimeUnit.MILLISECONDS);
        
        Assert.assertEquals("host-1", response.first());
        
        Assert.assertEquals(1, delayedResponse1.getExecuteCount());
        Assert.assertEquals(1, delayedResponse1.getOnSubscribeCount());
        Assert.assertEquals(0, delayedResponse1.getOnErrorCount());
        Assert.assertEquals(1, delayedResponse1.getOnNextCount());
        Assert.assertEquals(1, delayedResponse1.getOnCompletedCount());
        
        Assert.assertEquals(1, delayedResponse2.getExecuteCount());
        Assert.assertEquals(1, delayedResponse2.getOnSubscribeCount());
        Assert.assertEquals(0, delayedResponse2.getOnErrorCount());
        Assert.assertEquals(0, delayedResponse2.getOnNextCount());
        Assert.assertEquals(0, delayedResponse2.getOnCompletedCount());        
    }

    @Test
    public void secondRespondsAfterBackupTimeout() throws InterruptedException {
        hosts.onNext(slowResponse1);
        hosts.onNext(fastResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.execute(this.op).subscribe(response);
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, fastResponse1.getOnNextCount());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        
        Assert.assertEquals("host-2", response.first());
        
        Assert.assertEquals(1, slowResponse1.getExecuteCount());
        Assert.assertEquals(1, slowResponse1.getOnSubscribeCount());
        Assert.assertEquals(0, slowResponse1.getOnErrorCount());
        Assert.assertEquals(0, slowResponse1.getOnNextCount());
        Assert.assertEquals(0, slowResponse1.getOnCompletedCount());
        
        Assert.assertEquals(1, fastResponse2.getExecuteCount());
        Assert.assertEquals(1, fastResponse2.getOnSubscribeCount());
        Assert.assertEquals(0, fastResponse2.getOnErrorCount());
        Assert.assertEquals(1, fastResponse2.getOnNextCount());
        Assert.assertEquals(1, fastResponse2.getOnCompletedCount());        
    }

    @Test
    public void secondRespondsAfterFailedFirst() throws InterruptedException {
        hosts.onNext(fastError1);
        hosts.onNext(fastResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.execute(this.op).subscribe(response);
        
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, fastResponse1.getOnNextCount());
        
        Assert.assertTrue(fastError1.hasError());
        Assert.assertFalse(response.hasError());
        
        Assert.assertEquals(1, fastError1.getExecuteCount());
        Assert.assertEquals(1, fastError1.getOnSubscribeCount());
        Assert.assertEquals(1, fastError1.getOnErrorCount());
        Assert.assertEquals(0, fastError1.getOnNextCount());
        Assert.assertEquals(0, fastError1.getOnCompletedCount());
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, fastResponse2.getExecuteCount());
        Assert.assertEquals(1, fastResponse2.getOnSubscribeCount());
        Assert.assertEquals(0, fastResponse2.getOnErrorCount());
        Assert.assertEquals(1, fastResponse2.getOnNextCount());
        Assert.assertEquals(1, fastResponse2.getOnCompletedCount());        
    }
    
    @Test
    public void bothFail() throws InterruptedException {
        hosts.onNext(fastError1);
        hosts.onNext(fastError2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.execute(this.op).subscribe(response);
        
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, fastResponse1.getOnNextCount());
        
        Assert.assertTrue(fastError1.hasError());
        Assert.assertFalse(response.hasError());
        
        Assert.assertEquals(1, fastError1.getExecuteCount());
        Assert.assertEquals(1, fastError1.getOnSubscribeCount());
        Assert.assertEquals(1, fastError1.getOnErrorCount());
        Assert.assertEquals(0, fastError1.getOnNextCount());
        Assert.assertEquals(0, fastError1.getOnCompletedCount());
        Assert.assertFalse(response.hasError());
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, fastError2.getExecuteCount());
        Assert.assertEquals(1, fastError2.getOnSubscribeCount());
        Assert.assertEquals(1, fastError2.getOnErrorCount());
        Assert.assertEquals(0, fastError2.getOnNextCount());
        Assert.assertEquals(0, fastError2.getOnCompletedCount());        
        
        Assert.assertTrue(response.hasError());
        
    }
    
    @Test
    public void secondRespondsAfterFirstEmpty() {
        hosts.onNext(emptyResponse1);
        hosts.onNext(fastResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.execute(this.op).subscribe(response);
        
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        Assert.assertEquals(0, emptyResponse1.getOnNextCount());
        Assert.assertEquals(1, emptyResponse1.getOnCompletedCount());
        
        Assert.assertEquals(0, fastResponse2.getOnUnSubscribeCount());
        Assert.assertFalse(response.isCompleted());
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT+1, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, fastResponse2.getOnSubscribeCount());
        Assert.assertEquals(1, fastResponse2.getExecuteCount());
        Assert.assertEquals(1, fastResponse2.getOnNextCount());
        
        Assert.assertEquals(response.first(), "host-2");
    }
    
    @Test
    public void firstRespondsAfterSecondEmpty() {
        hosts.onNext(slowResponse1);
        hosts.onNext(emptyResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.execute(this.op).subscribe(response);
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT*3, TimeUnit.MILLISECONDS);
        
        Assert.assertEquals(1, emptyResponse2.getOnSubscribeCount());
        Assert.assertEquals(0, emptyResponse2.getOnNextCount());
        
        Assert.assertEquals(response.first(), "host-1");
    }
    
    @Test
    public void errorAfterBothEmpty() {
        hosts.onNext(emptyResponse1);
        hosts.onNext(emptyResponse2);

        TestObserver<String> response = new TestObserver<String>();
        this.executor.execute(this.op).subscribe(response);
        
        scheduler.advanceTimeBy(BACKUP_REQUEST_TIMEOUT+1, TimeUnit.MILLISECONDS);
        
        Assert.assertTrue(response.hasError());
    }
}
