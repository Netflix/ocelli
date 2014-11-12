package netflix.ocelli.client;

import java.util.concurrent.ConcurrentMap;

import netflix.ocelli.ClientConnector;
import rx.Observable;

import com.google.common.collect.Maps;

public class TestClientConnectorFactory implements ClientConnector<TestClient> {
    private ConcurrentMap<TestClient, TestClientConnector> connectors = Maps.newConcurrentMap();
    
    @Override
    public Observable<TestClient> call(TestClient client) {
        return Observable.create(get(client));
    }
    
    public TestClientConnector get(TestClient client) {
        TestClientConnector connector = new TestClientConnector(client);
        TestClientConnector prev = connectors.putIfAbsent(client, connector);
        if (prev != null) {
            connector = prev;
        }
        return connector;
    }

}
