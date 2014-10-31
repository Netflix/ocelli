package rx.loadbalancer.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import rx.Observable;
import rx.functions.Func1;
import rx.loadbalancer.util.RxUtil;

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
    
    public static Func1<TestClient, Observable<TestClient>> failure(final long amount, final TimeUnit units) {
        return new Func1<TestClient, Observable<TestClient>>() {
            @Override
            public Observable<TestClient> call(TestClient client) {
                return Observable
                        .just(client)
                        .delay(amount, units)
                        .ignoreElements()
                        .concatWith(Observable.<TestClient>error(new Exception("error")));
            }
        };
    }
    
    public static Func1<TestClient, Observable<TestClient>> failFirst(final int num) {
        return new Func1<TestClient, Observable<TestClient>>() {
            private int counter;
            @Override
            public Observable<TestClient> call(TestClient client) {
                if (counter++ < num) {
                    return Observable.error(new Exception("Failure-" + counter));
                }
                return Observable.just(client);
            }
        };
    }
    
    public static Func1<TestClient, Observable<TestClient>> failure() {
        return new Func1<TestClient, Observable<TestClient>>() {
            @Override
            public Observable<TestClient> call(TestClient client) {
                return Observable
                        .just(client)
                        .concatWith(Observable.<TestClient>error(new Exception("error")));
            }
        };
    }
    
    public static Func1<TestClient, Observable<TestClient>> degradation(final long initial, final long step, final TimeUnit units) {
        return new Func1<TestClient, Observable<TestClient>>() {
            private AtomicLong counter = new AtomicLong(0);
            
            @Override
            public Observable<TestClient> call(TestClient client) {
                return Observable
                        .just(client)
                        .delay(initial + counter.incrementAndGet() + step, units);
            }
        };
    }

    public static Func1<TestClient, Observable<TestClient>> proportionalToLoad(final long baseline, final long step, final TimeUnit units) {
        return new Func1<TestClient, Observable<TestClient>>() {
            private AtomicLong counter = new AtomicLong(0);
            
            @Override
            public Observable<TestClient> call(TestClient client) { 
                final long count = counter.incrementAndGet();
                return Observable
                        .just(client)
                        .delay(baseline + count + step, units)
                        .finallyDo(RxUtil.decrement(counter));
            }
        };
    }
    // public static poissonDelay()
    // public static gaussianDelay();
    // public static gcPauses();
}
