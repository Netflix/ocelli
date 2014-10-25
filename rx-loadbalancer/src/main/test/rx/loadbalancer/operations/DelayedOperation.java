package rx.loadbalancer.operations;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;
import rx.loadbalancer.client.TestClient;

public class DelayedOperation implements Func1<TestClient, Observable<String>> {
    private final long duration;
    private final TimeUnit units;
    
    public DelayedOperation(long duration, TimeUnit units) {
        this.duration = duration;
        this.units = units;
    }
    @Override
    public Observable<String> call(final TestClient server) {
        return Observable
                .interval(duration, units)
                .take(1)
                .map(new Func1<Long, String>() {
                    @Override
                    public String call(Long t1) {
                        return server.getHost() + "-ok";
                    }                        
                });
    }
}
