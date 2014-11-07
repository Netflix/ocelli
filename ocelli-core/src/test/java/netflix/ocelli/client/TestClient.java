package netflix.ocelli.client;

import netflix.ocelli.ClientEvent;
import netflix.ocelli.util.Stopwatch;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.concurrent.TimeUnit;

public class TestClient {
    private final TestHost host;
    private final Action1<ClientEvent> actions;
    
    public TestClient(TestHost host, Action1<ClientEvent> actions) {
        this.host = host;
        this.actions = actions;
    }
    
    public Observable<String> execute(Func1<TestClient, Observable<String>> operation) {
        final Stopwatch sw = Stopwatch.createStarted();
        actions.call(ClientEvent.requestStart());
        return host
            .execute(this, operation)
            .doOnEach(new Observer<String>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                    actions.call(ClientEvent.requestFailure(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS, e));
                }

                @Override
                public void onNext(String t) {
                    actions.call(ClientEvent.requestSuccess(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS));
                }
            });
    }
    
    public TestHost getHost() {
        return host;
    }
    
    public Action1<ClientEvent> actions() {
        return actions;
    }
    
    public String toString() {
        return "Client[" + host + "]";
    }

}
