package rx.loadbalancer.operations;

import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;
import rx.loadbalancer.client.TestClient;

public class TimeoutOperation implements Func1<TestClient, Observable<String>> {
    private final long duration;
    private final TimeUnit units;
    
    public TimeoutOperation(long duration, TimeUnit units) {
        this.duration = duration;
        this.units = units;
    }
    
    @Override
    public Observable<String> call(TestClient t1) {
        return Observable
                .interval(duration, units)
                .flatMap(new Func1<Long, Observable<String>>() {
                    @Override
                    public Observable<String> call(Long t1) {
                        return Observable.error(new SocketTimeoutException("Timeout"));
                    }
                });
    }
}