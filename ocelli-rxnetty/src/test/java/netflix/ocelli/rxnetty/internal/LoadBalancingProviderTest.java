package netflix.ocelli.rxnetty.internal;

import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.ConnectionFactory;
import io.reactivex.netty.protocol.tcp.client.ConnectionObservable;
import io.reactivex.netty.protocol.tcp.client.ConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.Instance;
import netflix.ocelli.rxnetty.FailureListener;
import netflix.ocelli.rxnetty.internal.AbstractLoadBalancer.LoadBalancingProvider;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.verification.VerificationMode;
import rx.Observable;
import rx.functions.Func1;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class LoadBalancingProviderTest {

    @Rule
    public final LoadBalancerRule loadBalancerRule = new LoadBalancerRule();

    @Test(timeout = 60000)
    public void testRoundRobin() throws Exception {
        List<Instance<SocketAddress>> hosts = loadBalancerRule.setupDefault();

        assertThat("Unexpected hosts found.", hosts, hasSize(2));

        AbstractLoadBalancer<String, String> loadBalancer = loadBalancerRule.getLoadBalancer();
        ConnectionFactory<String, String> cfMock = loadBalancerRule.newConnectionFactoryMock();

        Observable<Instance<ConnectionProvider<String, String>>> providers =
                loadBalancerRule.getHostsAsConnectionProviders(cfMock);

        LoadBalancingProvider lbProvider = newLoadBalancingProvider(loadBalancer, cfMock, providers);

        @SuppressWarnings("unchecked")
        ConnectionObservable<String, String> connectionObservable = lbProvider.nextConnection();

        assertNextConnection(hosts.get(0).getValue(), cfMock, connectionObservable, times(1));

        assertNextConnection(hosts.get(1).getValue(), cfMock, connectionObservable, times(1));

        assertNextConnection(hosts.get(0).getValue(), cfMock, connectionObservable,
                             times(2) /*Invoked once above with same host*/);
    }

    @Test(timeout = 60000)
    public void testListenerSubscription() throws Exception {
        final AtomicBoolean listenerCalled = new AtomicBoolean();
        final TcpClientEventListener listener = new TcpClientEventListener() {
            @Override
            public void onConnectionCloseStart() {
                listenerCalled.set(true);
            }
        };
        InetSocketAddress host = new InetSocketAddress(0);
        loadBalancerRule.setup(new Func1<FailureListener, TcpClientEventListener>() {
            @Override
            public TcpClientEventListener call(FailureListener failureListener) {
                return listener;
            }
        }, host);

        AbstractLoadBalancer<String, String> loadBalancer = loadBalancerRule.getLoadBalancer();
        ConnectionFactory<String, String> cfMock = loadBalancerRule.newConnectionFactoryMock();

        Observable<Instance<ConnectionProvider<String, String>>> providers =
                loadBalancerRule.getHostsAsConnectionProviders(cfMock);

        LoadBalancingProvider lbProvider = newLoadBalancingProvider(loadBalancer, cfMock, providers);

        @SuppressWarnings("unchecked")
        ConnectionObservable<String, String> connectionObservable = lbProvider.nextConnection();

        Connection<String, String> c = assertNextConnection(host, cfMock, connectionObservable, times(1));
        c.closeNow();

        assertThat("Listener not called.", listenerCalled.get(), is(true));
    }

    protected Connection<String, String> assertNextConnection(SocketAddress host,
                                                              ConnectionFactory<String, String> cfMock,
                                                              ConnectionObservable<String, String> connectionObservable,
                                                              VerificationMode verificationMode) {
        Connection<String, String> c = loadBalancerRule.connect(connectionObservable);
        verify(cfMock, verificationMode).newConnection(host);
        return c;
    }

    protected LoadBalancingProvider newLoadBalancingProvider(AbstractLoadBalancer<String, String> loadBalancer,
                                                             ConnectionFactory<String, String> cfMock,
                                                             Observable<Instance<ConnectionProvider<String, String>>> providers) {
        LoadBalancingProvider lbProvider = loadBalancer.new LoadBalancingProvider(cfMock, providers);
        TestSubscriber<Void> subscriber = new TestSubscriber<>();
        @SuppressWarnings("unchecked")
        Observable<Void> start = lbProvider.start();
        start.subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        return lbProvider;
    }
}