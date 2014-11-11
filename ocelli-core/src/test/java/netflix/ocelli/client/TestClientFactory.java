package netflix.ocelli.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.HostClientConnector;
import netflix.ocelli.metrics.ClientMetricsListener;
import netflix.ocelli.util.RxUtil;
import netflix.ocelli.util.Stopwatch;
import rx.Observable;
import rx.Observer;

public class TestClientFactory implements HostClientConnector<TestHost, TestClient> {
    private final AtomicInteger counter = new AtomicInteger();
    
    @Override
    public Observable<TestClient> call(final TestHost host, final ClientMetricsListener events, final Observable<Void> signal) {
        counter.incrementAndGet();
        
        final Stopwatch sw = Stopwatch.createStarted();
        events.onEvent(ClientEvent.CONNECT_START, 0, null, null, null);
        return host
                .connect()
                .cast(TestClient.class)
                .defaultIfEmpty(new TestClient(host, events))
                .doOnEach(new Observer<TestClient>() {
                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        events.onEvent(ClientEvent.CONNECT_FAILURE, sw.getRawElapsed(), TimeUnit.NANOSECONDS, e, null);
                    }

                    @Override
                    public void onNext(TestClient t) {
                        signal
                            .doOnCompleted(RxUtil.info("Client " + host + " removed"))
                            .subscribe();
                        
                        events.onEvent(ClientEvent.CONNECT_SUCCESS, sw.getRawElapsed(), TimeUnit.NANOSECONDS, null, t);
                    }
                });
    }
    
    public int getCallCount() {
        return counter.get();
    }
}
