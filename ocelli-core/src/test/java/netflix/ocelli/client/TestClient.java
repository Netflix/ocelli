package netflix.ocelli.client;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.metrics.ClientMetricsListener;
import netflix.ocelli.util.Stopwatch;
import rx.Observable;
import rx.Observer;
import rx.functions.Func1;

public class TestClient {
    private final TestHost host;
    private final ClientMetricsListener events;
    
    public TestClient(TestHost host, ClientMetricsListener actions) {
        this.host = host;
        this.events = actions;
    }
    
    public Observable<String> execute(Func1<TestClient, Observable<String>> operation) {
        final Stopwatch sw = Stopwatch.createStarted();
        events.onEvent(ClientEvent.REQUEST_START, 0, null, null, null);
        return host
            .execute(this, operation)
            .doOnEach(new Observer<String>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                    events.onEvent(ClientEvent.REQUEST_FAILURE, sw.getRawElapsed(), TimeUnit.NANOSECONDS, e, null);
                }

                @Override
                public void onNext(String t) {
                    events.onEvent(ClientEvent.REQUEST_SUCCESS, sw.getRawElapsed(), TimeUnit.NANOSECONDS, null, t);
                }
            });
    }
    
    public TestHost getHost() {
        return host;
    }
    
    public String toString() {
        return "Client[" + host + "]";
    }

}
