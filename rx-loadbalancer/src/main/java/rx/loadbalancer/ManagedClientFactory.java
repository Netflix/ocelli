package rx.loadbalancer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.functions.Action0;
import rx.functions.Action1;

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
public class ManagedClientFactory<H, C, M extends Action1<ClientEvent>> {
    private HostClientConnector<H, C>  connector;
    private MetricsFactory<H, M> metricsFactory;
    private ConcurrentMap<H, ManagedClient<H,C,M>> clients = new ConcurrentHashMap<H, ManagedClient<H, C, M>>();
    
    public ManagedClientFactory(HostClientConnector<H, C> connector, MetricsFactory<H, M> metricsFactory) {
        this.connector = connector;
        this.metricsFactory = metricsFactory;
    }

    public ManagedClient<H, C, M> create(final H h) {
        ManagedClient<H, C, M> client = clients.get(h);
        if (client == null) {
            final ManagedClient<H, C, M> newClient = new ManagedClient<H, C, M>(h, connector, metricsFactory);
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
    
    public ManagedClient<H, C, M> get(final H h) {
        return clients.get(h);
    }
}
