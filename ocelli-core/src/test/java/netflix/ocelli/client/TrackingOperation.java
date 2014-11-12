package netflix.ocelli.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;

public class TrackingOperation implements Func1<TestClient, Observable<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(TrackingOperation.class);
    
    private final String response;
    
    private List<TestClient> servers = new ArrayList<TestClient>();
    
    public TrackingOperation(String response) {
        this.response = response;
    }
    
    @Override
    public Observable<String> call(final TestClient client) {
        servers.add(client);
        return client.execute(new Func1<TestClient, Observable<String>>() {
            @Override
            public Observable<String> call(TestClient t1) {
                return Observable.just(response);
            }
        });
    }
    
    public List<TestClient> getServers() {
        return servers;
    }
}
