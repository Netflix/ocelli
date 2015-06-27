package netflix.ocelli.rxnetty.internal;

import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.channel.pool.PooledConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.ConnectionFactory;
import io.reactivex.netty.protocol.tcp.client.ConnectionObservable;
import io.reactivex.netty.protocol.tcp.client.ConnectionObservable.AbstractOnSubscribeFunc;
import io.reactivex.netty.protocol.tcp.client.ConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.Instance;
import netflix.ocelli.LoadBalancerStrategy;
import netflix.ocelli.rxnetty.FailureListener;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;

import java.net.SocketAddress;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An abstract load balancer for all TCP based protocols.
 *
 * <h2>Failure detection</h2>
 *
 * For every host that this load balancer connects, it provides a way to register a {@link TcpClientEventListener}
 * instance that can detect failures based on the various events received. Upon detecting the failure, an appropriate
 * action can be taken for the host, using the provided {@link FailureListener}.
 *
 * <h2>Use with RxNetty clients</h2>
 *
 * In order to use this load balancer with RxNetty clients, one has to convert it to an instance of
 * {@link ConnectionProvider} by calling {@link #toConnectionProvider()}
 *
 * @param <W> Type of Objects written on the connections created by this load balancer.
 * @param <R> Type of Objects read from the connections created by this load balancer.
 */
public abstract class AbstractLoadBalancer<W, R> {

    protected final Observable<Instance<SocketAddress>> hosts;
    protected final LoadBalancerStrategy<HostConnectionProvider<W, R>> loadBalancer;
    protected final Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory;

    protected AbstractLoadBalancer(Observable<Instance<SocketAddress>> hosts,
                                   Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory,
                                   LoadBalancerStrategy<HostConnectionProvider<W, R>> loadBalancer) {
        this.hosts = hosts;
        this.eventListenerFactory = eventListenerFactory;
        this.loadBalancer = loadBalancer;
    }

    /**
     * Converts this load balancer to a {@link ConnectionProvider} to be used with RxNetty clients.
     *
     * @return {@link ConnectionProvider} for this load balancer.
     */
    public ConnectionProvider<W, R> toConnectionProvider() {
        return ConnectionProvider.create(new Func1<ConnectionFactory<W, R>, ConnectionProvider<W, R>>() {
            @Override
            public ConnectionProvider<W, R> call(final ConnectionFactory<W, R> connectionFactory) {
                return toConnectionProvider(connectionFactory);
            }
        });
    }

    /*Visible for testing*/ ConnectionProvider<W, R> toConnectionProvider(final ConnectionFactory<W, R> factory) {

        final Observable<Instance<ConnectionProvider<W, R>>> providerStream =
                hosts.map(new Func1<Instance<SocketAddress>, Instance<ConnectionProvider<W, R>>>() {
                    @Override
                    public Instance<ConnectionProvider<W, R>> call(final Instance<SocketAddress> host) {
                        final ConnectionProvider<W, R> pcp = newConnectionProviderForHost(host, factory);

                        return new Instance<ConnectionProvider<W, R>>() {
                            @Override
                            public Observable<Void> getLifecycle() {
                                return host.getLifecycle();
                            }

                            @Override
                            public ConnectionProvider<W, R> getValue() {
                                return pcp;
                            }
                        };
                    }
                });

        return new LoadBalancingProvider(factory, providerStream);
    }

    protected ConnectionProvider<W, R> newConnectionProviderForHost(Instance<SocketAddress> host,
                                                                    ConnectionFactory<W, R> connectionFactory) {
        /*
         * Bounds on the concurrency (concurrent connections) should be enforced at the request
         * processing level, providing a bound on number of connections is a difficult number
         * to determine.
         */
        return PooledConnectionProvider.createUnbounded(connectionFactory, host.getValue());
    }

    /*Visible for testing*/class LoadBalancingProvider extends ConnectionProvider<W, R> {

        private final HostHolder<W, R> hostHolder;

        public LoadBalancingProvider(ConnectionFactory<W, R> connectionFactory,
                                     Observable<Instance<ConnectionProvider<W, R>>> providerStream) {
            super(connectionFactory);
            hostHolder = new HostHolder<>(providerStream, eventListenerFactory);
            hostHolder.start(); // TODO: RxNetty as of today does not call start.
        }

        @Override
        public ConnectionObservable<R, W> nextConnection() {

            return ConnectionObservable.createNew(new AbstractOnSubscribeFunc<R, W>() {
                @Override
                protected void doSubscribe(Subscriber<? super Connection<R, W>> sub,
                                           Action1<ConnectionObservable<R, W>> subscribeAllListenersAction) {
                    final List<HostConnectionProvider<W, R>> providers = hostHolder.getProviders();

                    if (null == providers || providers.isEmpty()) {
                        sub.onError(new NoSuchElementException("No hosts available."));
                    }

                    HostConnectionProvider<W, R> hcp = loadBalancer.choose(providers);

                    ConnectionObservable<R, W> nextConnection = hcp.getProvider().nextConnection();
                    if (hcp.getEventsListener() != null) {
                        nextConnection.subscribeForEvents(hcp.getEventsListener());
                    }

                    nextConnection.unsafeSubscribe(sub);
                }
            });
        }

        @Override
        public Observable<Void> start() {
            return hostHolder.start();
        }

        /*Visible for testing*/HostHolder<W, R> getHostHolder() {
            return hostHolder;
        }
    }
}
