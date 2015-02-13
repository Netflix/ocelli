package netflix.ocelli.client;

import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.functions.Func1;

import com.google.common.collect.Maps;

public class TestClientConnectorFactory implements Func1<TestClient, Observable<Void>> {
    private ConcurrentMap<TestClient, TestClientConnector> connectors = Maps.newConcurrentMap();
    
    @Override
    public Observable<Void> call(TestClient client) {
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
