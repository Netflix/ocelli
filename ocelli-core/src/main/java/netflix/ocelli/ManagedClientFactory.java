package netflix.ocelli;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.functions.Action0;

/**
 * Factory for creating managed clients.  Clients are cached by host so that the hosts
 * may be shared accross multiple load balancers.
 * 
 * @author elandau
 *
 * @param <H>
 * @param <C>
 * @param <M>
 */
public class ManagedClientFactory<H, C> {
    private final HostClientConnector<H, C>  connector;
    private final List<MetricsFactory<H>> metricsFactory;
    private final ConcurrentMap<H, ManagedClient<H,C>> clients = new ConcurrentHashMap<H, ManagedClient<H, C>>();
    
    public ManagedClientFactory(HostClientConnector<H, C> connector, List<MetricsFactory<H>> metricsFactory) {
        this.connector = connector;
        this.metricsFactory = metricsFactory;
    }

    public ManagedClient<H, C> create(final H h) {
        ManagedClient<H, C> client = clients.get(h);
        if (client == null) {
            final ManagedClient<H, C> newClient = new ManagedClient<H, C>(h, connector, metricsFactory);
            client = clients.putIfAbsent(h, newClient);
            if (client == null) {
                client = newClient;
                
                // onComplete means that the client is no longer used by any pool and can 
                // be removed from the map
                client.notifications().doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        clients.remove(h, newClient);
                    }
                });
            }
        }
        return client;
    }
    
    public ManagedClient<H, C> get(final H h) {
        return clients.get(h);
    }
}
