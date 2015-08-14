package netflix.ocelli.rxnetty.internal;

import io.netty.channel.embedded.EmbeddedChannel;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.channel.ConnectionImpl;
import io.reactivex.netty.client.ConnectionFactory;
import io.reactivex.netty.client.ConnectionObservable;
import io.reactivex.netty.client.ConnectionObservable.OnSubcribeFunc;
import io.reactivex.netty.client.ConnectionProvider;
import io.reactivex.netty.client.events.ClientEventListener;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventPublisher;
import netflix.ocelli.Instance;
import netflix.ocelli.LoadBalancerStrategy;
import netflix.ocelli.loadbalancer.RoundRobinLoadBalancer;
import netflix.ocelli.rxnetty.FailureListener;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.Mockito;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Func1;
import rx.functions.Func3;
import rx.observers.TestSubscriber;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class LoadBalancerRule extends ExternalResource {

    private Observable<Instance<SocketAddress>> hosts;
    private Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory;
    private LoadBalancerStrategy<HostConnectionProvider<String, String>> loadBalancingStratgey;
    private AbstractLoadBalancer<String, String> loadBalancer;
    private Func3<Observable<Instance<SocketAddress>>, Func1<FailureListener, ? extends TcpClientEventListener>,
            LoadBalancerStrategy<HostConnectionProvider<String, String>>, AbstractLoadBalancer<String, String>> lbFactory;

    public LoadBalancerRule() {
    }

    public LoadBalancerRule(Func3<Observable<Instance<SocketAddress>>,
                                    Func1<FailureListener, ? extends TcpClientEventListener>,
                                    LoadBalancerStrategy<HostConnectionProvider<String, String>>,
                                    AbstractLoadBalancer<String, String>> lbFactory) {
        this.lbFactory = lbFactory;
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                base.evaluate();
            }
        };
    }

    public AbstractLoadBalancer<String, String> getLoadBalancer() {
        return loadBalancer;
    }

    public List<Instance<SocketAddress>> setupDefault() {
        final List<Instance<SocketAddress>> instances = new ArrayList<>();
        instances.add(new DummyInstance());
        instances.add(new DummyInstance());
        setup(instances.get(0).getValue(), instances.get(1).getValue());
        return hosts.toList().toBlocking().single();
    }

    public AbstractLoadBalancer<String, String> setup(SocketAddress... hosts) {
        return setup(new Func1<FailureListener, TcpClientEventListener>() {
            @Override
            public TcpClientEventListener call(FailureListener failureListener) {
                return null;
            }
        }, hosts);
    }

    public AbstractLoadBalancer<String, String> setup(
            Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory,
            SocketAddress... hosts) {
        return setup(eventListenerFactory, new RoundRobinLoadBalancer<HostConnectionProvider<String, String>>(-1),
                     hosts);
    }

    public AbstractLoadBalancer<String, String> setup(
            Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory,
            LoadBalancerStrategy<HostConnectionProvider<String, String>> loadBalancingStratgey,
            SocketAddress... hosts) {
        List<Instance<SocketAddress>> instances = new ArrayList<>(hosts.length);
        for (SocketAddress host : hosts) {
            instances.add(new DummyInstance(host));
        }
        return setup(eventListenerFactory, loadBalancingStratgey, instances);
    }

    public AbstractLoadBalancer<String, String> setup(
            Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory,
            LoadBalancerStrategy<HostConnectionProvider<String, String>> loadBalancingStratgey,
            List<Instance<SocketAddress>> hosts) {
        this.hosts = Observable.from(hosts);
        this.eventListenerFactory = eventListenerFactory;
        this.loadBalancingStratgey = loadBalancingStratgey;

        if (null != lbFactory) {
            loadBalancer = lbFactory.call(this.hosts, eventListenerFactory, loadBalancingStratgey);
            return loadBalancer;
        }

        loadBalancer = new AbstractLoadBalancer<String, String>(this.hosts, eventListenerFactory,
                                                                loadBalancingStratgey) {
            @Override
            protected ConnectionProvider<String, String> newConnectionProviderForHost(final Instance<SocketAddress> host,
                                                                                      final ConnectionFactory<String, String> connectionFactory) {
                return new ConnectionProvider<String, String>(connectionFactory) {
                    @Override
                    public ConnectionObservable<String, String> nextConnection() {
                        return connectionFactory.newConnection(host.getValue());
                    }
                };
            }
        };
        return getLoadBalancer();
    }

    public Func1<FailureListener, ? extends TcpClientEventListener> getEventListenerFactory() {
        return eventListenerFactory;
    }

    public Observable<Instance<SocketAddress>> getHosts() {
        return hosts;
    }

    public Observable<Instance<ConnectionProvider<String, String>>> getHostsAsConnectionProviders(
            final ConnectionFactory<String, String> cfMock) {
        return hosts.map(new Func1<Instance<SocketAddress>, Instance<ConnectionProvider<String, String>>>() {
            @Override
            public Instance<ConnectionProvider<String, String>> call(final Instance<SocketAddress> i) {
                final ConnectionProvider<String, String> cp = new ConnectionProvider<String, String>(cfMock) {
                    @Override
                    public ConnectionObservable<String, String> nextConnection() {
                        return cfMock.newConnection(i.getValue());
                    }
                };
                return new Instance<ConnectionProvider<String, String>>() {
                    @Override
                    public Observable<Void> getLifecycle() {
                        return i.getLifecycle();
                    }

                    @Override
                    public ConnectionProvider<String, String> getValue() {
                        return cp;
                    }
                };
            }
        });
    }

    public LoadBalancerStrategy<HostConnectionProvider<String, String>> getLoadBalancingStratgey() {
        return loadBalancingStratgey;
    }

    public Connection<String, String> connect(ConnectionObservable<String, String> connectionObservable) {

        TestSubscriber<Connection<String, String>> testSub = new TestSubscriber<>();
        connectionObservable.subscribe(testSub);

        testSub.awaitTerminalEvent();
        testSub.assertNoErrors();

        testSub.assertValueCount(1);

        return testSub.getOnNextEvents().get(0);
    }
    public ConnectionFactory<String, String> newConnectionFactoryMock() {
        @SuppressWarnings("unchecked")
        final
        ConnectionFactory<String, String> cfMock = Mockito.mock(ConnectionFactory.class);

        List<Instance<SocketAddress>> instances = hosts.toList().toBlocking().single();

        for (Instance<SocketAddress> instance : instances) {
            EmbeddedChannel channel = new EmbeddedChannel();
            final TcpClientEventPublisher eventPublisher = new TcpClientEventPublisher();
            final Connection<String, String> mockConnection =
                    ConnectionImpl.create(channel, eventPublisher, eventPublisher);

            Mockito.when(cfMock.newConnection(instance.getValue()))
                   .thenReturn(ConnectionObservable.createNew(new OnSubcribeFunc<String, String>() {
                       @Override
                       public Subscription subscribeForEvents(ClientEventListener eventListener) {
                           return eventPublisher.subscribe((TcpClientEventListener) eventListener);
                       }

                       @Override
                       public void call(Subscriber<? super Connection<String, String>> subscriber) {
                           subscriber.onNext(mockConnection);
                           subscriber.onCompleted();
                       }
                   }));
        }

        return cfMock;
    }

    private static class DummyInstance extends Instance<SocketAddress> {

        private final SocketAddress socketAddress;

        private DummyInstance() {
            socketAddress = new SocketAddress() {

                private static final long serialVersionUID = 711795406919943230L;

                @Override
                public String toString() {
                    return "Dummy socket address: " + hashCode();
                }
            };
        }

        private DummyInstance(SocketAddress socketAddress) {
            this.socketAddress = socketAddress;
        }


        @Override
        public Observable<Void> getLifecycle() {
            return Observable.never();
        }

        @Override
        public SocketAddress getValue() {
            return socketAddress;
        }
    }
}