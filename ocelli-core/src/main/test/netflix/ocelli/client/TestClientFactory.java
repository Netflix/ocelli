package netflix.ocelli.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.HostClientConnector;
import netflix.ocelli.util.RxUtil;
import netflix.ocelli.util.Stopwatch;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;

public class TestClientFactory implements HostClientConnector<TestHost, TestClient> {
    private final AtomicInteger counter = new AtomicInteger();
    
    @Override
    public Observable<TestClient> call(final TestHost host, final Action1<ClientEvent> events, final Observable<Void> signal) {
        counter.incrementAndGet();
        
        final Stopwatch sw = Stopwatch.createStarted();
        events.call(ClientEvent.connectStart());
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
                        events.call(ClientEvent.connectFailure(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS, e));
                    }

                    @Override
                    public void onNext(TestClient t) {
                        signal
                            .doOnCompleted(RxUtil.info("Client " + host + " removed"))
                            .subscribe();
                        
                        events.call(ClientEvent.connectSuccess(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS));
                    }
                });
    }
    
    public int getCallCount() {
        return counter.get();
    }
}
