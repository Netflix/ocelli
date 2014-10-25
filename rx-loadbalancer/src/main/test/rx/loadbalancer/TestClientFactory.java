package rx.loadbalancer;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.loadbalancer.client.TestClient;
import rx.loadbalancer.client.TestHost;
import rx.loadbalancer.util.Stopwatch;

public class TestClientFactory implements HostClientConnector<TestHost, TestClient> {
    @Override
    public Observable<TestClient> call(TestHost host, final Action1<ClientEvent> actions) {
        final Stopwatch sw = Stopwatch.createStarted();
        actions.call(ClientEvent.connectStart());
        return host
                .connect()
                .cast(TestClient.class)
                .defaultIfEmpty(new TestClient(host, actions))
                .doOnEach(new Observer<TestClient>() {
                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        actions.call(ClientEvent.connectFailure(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS, e));
                    }

                    @Override
                    public void onNext(TestClient t) {
                        actions.call(ClientEvent.connectSuccess(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS));
                    }
                });
    }
}
