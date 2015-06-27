package netflix.ocelli.rxnetty.protocol.tcp;

import io.reactivex.netty.protocol.tcp.client.ConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.Instance;
import netflix.ocelli.LoadBalancerStrategy;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import netflix.ocelli.rxnetty.FailureListener;
import netflix.ocelli.rxnetty.internal.AbstractLoadBalancer;
import netflix.ocelli.rxnetty.internal.HostConnectionProvider;
import rx.Observable;
import rx.functions.Func1;

import java.net.SocketAddress;

/**
 * An HTTP load balancer to be used with {@link TcpClient}.
 *
 * <h2>Failure detection</h2>
 *
 * For every host that this load balancer connects, it provides a way to register a {@link TcpClientEventListener}
 * instance that can detect failures based on the various events received. Upon detecting the failure, an appropriate
 * action can be taken for the host, using the provided {@link FailureListener}.
 *
 * <h2>Use with {@link TcpClient}</h2>
 *
 * In order to use this load balancer with RxNetty clients, one has to convert it to an instance of
 * {@link ConnectionProvider} by calling {@link #toConnectionProvider()}
 *
 * @param <W> Type of Objects written on the connections created by this load balancer.
 * @param <R> Type of Objects read from the connections created by this load balancer.
 */
public class TcpLoadBalancer<W, R> extends AbstractLoadBalancer<W, R> {

    private static final Func1<FailureListener, TcpClientEventListener> NO_LISTENER_FACTORY =
            new Func1<FailureListener, TcpClientEventListener>() {
                @Override
                public TcpClientEventListener call(FailureListener failureListener) {
                    return null;
                }
            };

    private TcpLoadBalancer(Observable<Instance<SocketAddress>> hosts,
                            LoadBalancerStrategy<HostConnectionProvider<W, R>> loadBalancer) {
        this(hosts, loadBalancer, NO_LISTENER_FACTORY);
    }

    private TcpLoadBalancer(Observable<Instance<SocketAddress>> hosts,
                            LoadBalancerStrategy<HostConnectionProvider<W, R>> loadBalancer,
                            Func1<FailureListener, TcpClientEventListener> eventListenerFactory) {
        super(hosts, eventListenerFactory, loadBalancer);
    }

    /**
     * Creates a new load balancer using a round-robin load balancing strategy ({@link RoundRobinLoadBalancer}) over the
     * passed stream of hosts. The hosts ({@link SocketAddress}) emitted by the passed stream are used till their
     * lifecycle ends ({@code Observable} returned by {@link Instance#getLifecycle()} terminates).
     *
     * For using any failure detection schemes for the hosts, use {@link #roundRobin(Observable, Func1)} instead.
     *
     * @param hosts Stream of hosts to use for load balancing.
     *
     * @param <W> Type of Objects written on the connections created by this load balancer.
     * @param <R> Type of Objects read from the connections created by this load balancer.
     *
     * @return New load balancer instance.
     */
    public static <W, R> TcpLoadBalancer<W, R> roundRobin(Observable<Instance<SocketAddress>> hosts) {
        return new TcpLoadBalancer<>(hosts, new RoundRobinLoadBalancer<HostConnectionProvider<W, R>>());
    }

    /**
     * Creates a new load balancer using a round-robin load balancing strategy ({@link RoundRobinLoadBalancer}) over the
     * passed stream of hosts. The hosts ({@link SocketAddress}) emitted by the passed stream are used till their
     * lifecycle ends ({@code Observable} returned by {@link Instance#getLifecycle()} terminates) or are explicitly
     * removed by the passed failure detector.
     *
     * @param hosts Stream of hosts to use for load balancing.
     * @param failureDetector A factory for creating a {@link TcpClientEventListener} per host. The listeners based on
     * any criterion can then remove the host from the load balancing pool.
     *
     * @param <W> Type of Objects written on the connections created by this load balancer.
     * @param <R> Type of Objects read from the connections created by this load balancer.
     *
     * @return New load balancer instance.
     */
    public static <W, R> TcpLoadBalancer<W, R> roundRobin(Observable<Instance<SocketAddress>> hosts,
                                                          Func1<FailureListener, TcpClientEventListener> failureDetector) {
        return new TcpLoadBalancer<>(hosts, new RoundRobinLoadBalancer<HostConnectionProvider<W, R>>(),
                                      failureDetector);
    }

}
