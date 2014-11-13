package netflix.ocelli.rxnetty;

import io.reactivex.netty.client.RxClient;
import netflix.ocelli.Host;
import netflix.ocelli.LoadBalancer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A pool of {@link RxClient} instances that can be used with {@link LoadBalancer} to reuse {@link RxClient} instances
 * created per {@link Host}.
 *
 * @author Nitesh Kant
 */
public abstract class RxNettyClientPool<I, O, T extends RxClient<I, O>> {

    private final ConcurrentHashMap<Host, T> clients = new ConcurrentHashMap<Host, T>();

    public T getClientForHost(Host host) {
        T client = clients.get(host);
        if (null == client) {
            client = createClient(host);
            T existing = clients.putIfAbsent(host, client);
            if (null != existing) {
                client.shutdown(); // Shutdown the client which did not get put in the map.
                client = existing;
            }
        }

        return client;
    }

    protected abstract T createClient(Host host);
}
