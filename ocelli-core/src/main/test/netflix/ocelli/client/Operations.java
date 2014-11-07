package netflix.ocelli.client;

import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;

public class Operations {
    
    public static Func1<TestClient, Observable<String>> delayed(final long duration, final TimeUnit units) {
        return new Func1<TestClient, Observable<String>>() {
            @Override
            public Observable<String> call(final TestClient server) {
                return Observable
                        .interval(duration, units)
                        .first()
                        .map(new Func1<Long, String>() {
                            @Override
                            public String call(Long t1) {
                                return server.getHost() + "-ok";
                            }                        
                        });
            }
        };
    }

    public static Func1<TestClient, Observable<String>> timeout(final long duration, final TimeUnit units) {
        return new Func1<TestClient, Observable<String>>() {
            @Override
            public Observable<String> call(final TestClient server) {
                return Observable
                        .interval(duration, units)
                        .flatMap(new Func1<Long, Observable<String>>() {
                            @Override
                            public Observable<String> call(Long t1) {
                                return Observable.error(new SocketTimeoutException("Timeout"));
                            }
                        });
                }
        };
    }

    public static TrackingOperation tracking(String response) {
        return new TrackingOperation(response);
    }
}
