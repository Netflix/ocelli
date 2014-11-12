package netflix.ocelli.client;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;

import netflix.ocelli.MetricsFactory;
import netflix.ocelli.util.RxUtil;
import rx.Observable;
import rx.functions.Func1;

public class TestClient {
    private final String id;
    private final Func1<TestClient, Observable<TestClient>> behavior;
    private final Observable<Void> connect;
    private final int concurrency = 10;
    private final Semaphore sem = new Semaphore(concurrency);
    private final Set<String> vips = new HashSet<String>();
    private String rack;
    
    public static MetricsFactory<TestClient, TestClient> metricsFactory() {
        return new MetricsFactory<TestClient, TestClient>() {
            @Override
            public Observable<TestClient> call(TestClient t1) {
                return Observable.just(t1);
            }
        };
    }
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

    public static TestClient create(String id, Observable<Void> connect, Func1<TestClient, Observable<TestClient>> behavior) {
        return new TestClient(id, connect, behavior);
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
    
    public Observable<String> execute(Func1<TestClient, Observable<String>> operation) {
        return behavior.call(this)
                .doOnSubscribe(RxUtil.acquire(sem))
                .concatMap(behavior)
                .concatMap(operation)
                .doOnCompleted(RxUtil.release(sem));
    }
    
    public String toString() {
        return "Host[id=" + id + ", pending=" + (concurrency - sem.availablePermits()) + ", vip=" + vips + " rack=" + rack + "]";
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
