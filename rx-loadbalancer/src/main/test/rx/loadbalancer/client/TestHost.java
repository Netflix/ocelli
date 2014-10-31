package rx.loadbalancer.client;

import java.util.concurrent.Semaphore;

import rx.Observable;
import rx.functions.Func1;
import rx.loadbalancer.util.RxUtil;

public class TestHost {
    private final String id;
    private final Func1<TestClient, Observable<TestClient>> behavior;
    private final Observable<Void> connect;
    private final int concurrency = 10;
    private final Semaphore sem = new Semaphore(concurrency);
    
    public static TestHost create(String id, Observable<Void> connect, Func1<TestClient, Observable<TestClient>> behavior) {
        return new TestHost(id, connect, behavior);
    }
    
    public TestHost(String id, Observable<Void> connect, Func1<TestClient, Observable<TestClient>> behavior) {
        this.id = id;
        this.behavior = behavior;
        this.connect = connect;
    }
    
    public Observable<Void> connect() {
        return connect;
    }
    
    public Observable<String> execute(TestClient client, Func1<TestClient, Observable<String>> operation) {
        return Observable
                .just(client)
                .doOnSubscribe(RxUtil.acquire(sem))
                .concatMap(behavior)
                .concatMap(operation)
                .doOnCompleted(RxUtil.release(sem))
                ;
    }
    
    public String toString() {
        return "Host[" + id + " pending=" + (concurrency - sem.availablePermits()) + "]";
    }

}
