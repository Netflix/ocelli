package rx.loadbalancer.client;

import rx.Observable;
import rx.functions.Func1;
import rx.loadbalancer.util.RxUtil;

public class TestHost {
    private final String id;
    private final Func1<TestClient, Observable<TestClient>> behavior;
    private final Observable<Void> connect;
    
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
    
    public String toString() {
        return "Host[" + id + "]";
    }

    public Observable<String> execute(TestClient client, Func1<TestClient, Observable<String>> operation) {
        return Observable
                .just(client)
                .doOnNext(RxUtil.info("Trying"))
                .concatMap(behavior)
                .concatMap(operation);
    }
}
