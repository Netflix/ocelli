package netflix.ocelli.client;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import netflix.ocelli.HostToClientMapper;
import netflix.ocelli.MemberToInstance;
import netflix.ocelli.util.RxUtil;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

public class TestClient {
    private final String id;
    private final Func1<TestClient, Observable<TestClient>> behavior;
    private final Observable<Void> connect;
    private final int concurrency = 10;
    private final Semaphore sem = new Semaphore(concurrency);
    private final Set<String> vips = new HashSet<String>();
    private String rack;
    
    public static Func1<TestClient, Observable<String>> byVip() {
        return new Func1<TestClient, Observable<String>>() {
            @Override
            public Observable<String> call(TestClient t1) {
                return Observable.from(t1.vips).concatWith(Observable.just("*"));
            }
        };
    }
    
    public static Func1<TestClient, Observable<String>> byRack() {
        return new Func1<TestClient, Observable<String>>() {
            @Override
            public Observable<String> call(TestClient t1) {
                return Observable.just(t1.rack);
            }
        };
    }
    
    public static Func1<TestClient, Integer> byPendingRequestCount() {
        return new Func1<TestClient, Integer>() {
            @Override
            public Integer call(TestClient t1) {
                return t1.sem.availablePermits();
            }
        };
    }
    
    public static MemberToInstance<TestClient, TestClient> memberToInstance(Func1<TestClient, Observable<Boolean>> failureDetectorFactory) {
        return new MemberToInstance<TestClient, TestClient>(new HostToClientMapper<TestClient, TestClient>(
            new Func1<TestClient, TestClient>() {
                @Override
                public TestClient call(TestClient t1) {
                    return t1;
                }
            },
            new Action1<TestClient>() {
                @Override
                public void call(TestClient t1) {
                }
            },
            failureDetectorFactory));
    }

    public static TestClient create(String id, Observable<Void> connect, Func1<TestClient, Observable<TestClient>> behavior) {
        return new TestClient(id, connect, behavior);
    }
    
    public static TestClient create(String id, Func1<TestClient, Observable<TestClient>> behavior) {
        return new TestClient(id, Connects.immediate(), behavior);
    }
    
    public static Func2<TestClient, String, Observable<String>> func() {
        return new Func2<TestClient, String,Observable<String>>() {
            @Override
            public Observable<String> call(TestClient client, String request) {
                return client.execute(new Func1<TestClient, Observable<String>>() {
                    @Override
                    public Observable<String> call(TestClient t1) {
                        return Observable.just(t1.id());
                    }
                });
            }
        };
    }
    
    public TestClient(String id, Observable<Void> connect, Func1<TestClient, Observable<TestClient>> behavior) {
        this.id = id;
        this.behavior = behavior;
        this.connect = connect;
    }
    
    public Observable<Void> connect() {
        return connect;
    }
    
    public TestClient withVip(String vip) {
        this.vips.add(vip);
        return this;
    }
    
    public TestClient withRack(String rack) {
        this.rack = rack;
        return this;
    }

    public Set<String> vips() {
        return this.vips;
    }
    
    public String rack() {
        return this.rack;
    }
    
    public String id() {
        return this.id;
    }
    
    private AtomicLong executeCount = new AtomicLong(0);
    private AtomicLong onNextCount = new AtomicLong(0);
    private AtomicLong onCompletedCount = new AtomicLong(0);
    private AtomicLong onSubscribeCount = new AtomicLong(0);
    private AtomicLong onUnSubscribeCount = new AtomicLong(0);
    private AtomicLong onErrorCount = new AtomicLong(0);
    
    public long getExecuteCount() {
        return executeCount.get();
    }
    
    public long getOnNextCount() {
        return onNextCount.get();
    }
    
    public long getOnCompletedCount() {
        return onCompletedCount.get();
    }
    
    public long getOnErrorCount() {
        return onErrorCount.get();
    }
    
    public long getOnSubscribeCount() {
        return onSubscribeCount.get();
    }
    
    public long getOnUnSubscribeCount() {
        return onUnSubscribeCount.get();
    }

    
    public boolean hasError() {
        return onErrorCount.get() > 0;
    }

    public Observable<String> execute(Func1<TestClient, Observable<String>> operation) {
        this.executeCount.incrementAndGet();
        return behavior.call(this)
                .doOnSubscribe(RxUtil.increment(onSubscribeCount))
                .doOnSubscribe(RxUtil.acquire(sem))
                .doOnUnsubscribe(RxUtil.increment(onUnSubscribeCount))
                .concatMap(operation)
                .doOnEach(new Observer<String>() {
                    @Override
                    public void onCompleted() {
                        onCompletedCount.incrementAndGet();
                        sem.release();
                    }

                    @Override
                    public void onError(Throwable e) {
                        onErrorCount.incrementAndGet();
                    }

                    @Override
                    public void onNext(String t) {
                        onNextCount.incrementAndGet();
                    }
                });
    }
    
    public String toString() {
//      return "Host[id=" + id + ", pending=" + (concurrency - sem.availablePermits()) + ", vip=" + vips + " rack=" + rack + "]";
        return "Host[id=" + id + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TestClient other = (TestClient) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

}
