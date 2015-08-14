package netflix.ocelli.rxnetty.internal;

import io.reactivex.netty.client.ConnectionFactory;
import io.reactivex.netty.client.ConnectionObservable;
import io.reactivex.netty.client.ConnectionProvider;
import netflix.ocelli.Instance;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.SocketAddress;
import java.util.List;

public class AbstractLoadBalancerTest {

    @Rule
    public final LoadBalancerRule lbRule = new LoadBalancerRule();

    @Test(timeout = 60000)
    public void testRoundRobin() throws Exception {
        List<Instance<SocketAddress>> hosts = lbRule.setupDefault();
        AbstractLoadBalancer<String, String> loadBalancer = lbRule.getLoadBalancer();
        ConnectionFactory<String, String> cfMock = lbRule.newConnectionFactoryMock();
        ConnectionProvider<String, String> cp = loadBalancer.toConnectionProvider(cfMock);

        ConnectionObservable<String, String> co = cp.nextConnection();

        lbRule.connect(co);
        Mockito.verify(cfMock).newConnection(hosts.get(0).getValue());
        Mockito.verifyNoMoreInteractions(cfMock);

        cp = loadBalancer.toConnectionProvider(cfMock);
        co = cp.nextConnection();
        lbRule.connect(co);
        Mockito.verify(cfMock).newConnection(hosts.get(1).getValue());
        Mockito.verifyNoMoreInteractions(cfMock);
    }
}