package rx.loadbalancer.operations;

import java.util.ArrayList;
import java.util.List;

import rx.Observable;
import rx.functions.Func1;
import rx.loadbalancer.client.TestClient;
import rx.loadbalancer.client.TestHost;

public class TrackingOperation implements Func1<TestClient, Observable<String>> {
    private final String response;
    
    private List<TestHost> servers = new ArrayList<TestHost>();
    
    public TrackingOperation(String response) {
        this.response = response;
    }
    
    @Override
    public Observable<String> call(final TestClient client) {
        servers.add(client.getHost());
        return client.execute(new Func1<TestClient, Observable<String>>() {
            @Override
            public Observable<String> call(TestClient t1) {
                return Observable.just(response);
            }
        });
    }
    
    public List<TestHost> getServers() {
        return servers;
    }
}
