package rx.loadbalancer.client;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;

public class Behaviors {
    public static Func1<TestClient, Observable<TestClient>> delay(final long amount, final TimeUnit units) {
        return new Func1<TestClient, Observable<TestClient>>() {
            @Override
            public Observable<TestClient> call(TestClient client) {
                return Observable
                        .just(client)
                        .delay(amount, units);
            }
        };
    }
    
    public static Func1<TestClient, Observable<TestClient>> immediate() {
        return new Func1<TestClient, Observable<TestClient>>() {
            @Override
            public Observable<TestClient> call(TestClient client) {
                return Observable
                        .just(client);
            }
        };
    }
    
    public static Func1<TestClient, Observable<TestClient>> failure(final long amount, final TimeUnit units, final Exception e) {
        return new Func1<TestClient, Observable<TestClient>>() {
            @Override
            public Observable<TestClient> call(TestClient client) {
                return Observable
                        .just(client)
                        .delay(amount, units)
                        .ignoreElements()
                        .concatWith(Observable.<TestClient>error(e));
            }
        };
    }
}
