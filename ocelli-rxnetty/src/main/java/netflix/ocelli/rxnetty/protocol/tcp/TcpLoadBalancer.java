package netflix.ocelli.rxnetty.protocol.tcp;

import io.reactivex.netty.client.ConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.Instance;
import netflix.ocelli.LoadBalancerStrategy;
import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;
import netflix.ocelli.loadbalancer.RandomWeightedLoadBalancer;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import netflix.ocelli.loadbalancer.weighting.LinearWeightingStrategy;
import netflix.ocelli.rxnetty.FailureListener;
import netflix.ocelli.rxnetty.internal.AbstractLoadBalancer;
import netflix.ocelli.rxnetty.internal.HostConnectionProvider;
import netflix.ocelli.rxnetty.protocol.WeightComparator;
import netflix.ocelli.rxnetty.protocol.http.WeightedHttpClientListener;
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

    /**
     * Typically, static methods in this class would be used to create new instances of the load balancer with known
     * load balancing strategies. However, for any custom load balancing schemes, one can use this constructor directly.
     *
     * @param hosts Stream of hosts to use for load balancing.
     * @param loadBalancer The load balancing strategy.
     * @param eventListenerFactory A factory for creating new {@link TcpClientEventListener} per host.
     */
    public TcpLoadBalancer(Observable<Instance<SocketAddress>> hosts,
                           LoadBalancerStrategy<HostConnectionProvider<W, R>> loadBalancer,
                           Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory) {
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


    /**
     * Creates a new load balancer using a weighted random load balancing strategy ({@link RandomWeightedLoadBalancer})
     * over the passed stream of hosts.
     *
     * @param hosts Stream of hosts to use for load balancing.
     * @param listenerFactory A factory for creating {@link WeightedTcpClientListener} per active host.
     *
     * @param <W> Type of Objects written on the connections created by this load balancer.
     * @param <R> Type of Objects read from the connections created by this load balancer.
     *
     * @return New load balancer instance.
     */
    public static <W, R> TcpLoadBalancer<W, R> weigthedRandom(Observable<Instance<SocketAddress>> hosts,
                                                              Func1<FailureListener, WeightedTcpClientListener> listenerFactory) {
        LinearWeightingStrategy<HostConnectionProvider<W, R>> ws = new LinearWeightingStrategy<>(
                new Func1<HostConnectionProvider<W, R>, Integer>() {
                    @Override
                    public Integer call(HostConnectionProvider<W, R> cp) {
                        WeightedHttpClientListener el = (WeightedHttpClientListener) cp.getEventsListener();
                        return el.getWeight();
                    }
                });
        return new TcpLoadBalancer<W, R>(hosts, new RandomWeightedLoadBalancer<HostConnectionProvider<W, R>>(ws),
                                          listenerFactory);
    }

    /**
     * Creates a new load balancer using a power of two choices load balancing strategy ({@link ChoiceOfTwoLoadBalancer})
     * over the passed stream of hosts.
     *
     * @param hosts Stream of hosts to use for load balancing.
     * @param listenerFactory A factory for creating {@link WeightedTcpClientListener} per active host.
     *
     * @param <W> Type of Objects written on the connections created by this load balancer.
     * @param <R> Type of Objects read from the connections created by this load balancer.
     *
     * @return New load balancer instance.
     */
    public static <W, R> TcpLoadBalancer<W, R> choiceOfTwo(Observable<Instance<SocketAddress>> hosts,
                                                           Func1<FailureListener, WeightedTcpClientListener> listenerFactory) {
        return new TcpLoadBalancer<W, R>(hosts, new ChoiceOfTwoLoadBalancer<>(new WeightComparator<W, R>()),
                                         listenerFactory);
    }

}
