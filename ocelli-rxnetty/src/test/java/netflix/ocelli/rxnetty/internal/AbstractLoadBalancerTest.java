package netflix.ocelli.rxnetty.internal;

import io.reactivex.netty.protocol.tcp.client.ConnectionFactory;
import io.reactivex.netty.protocol.tcp.client.ConnectionObservable;
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
        ConnectionObservable<String, String> co = loadBalancer.toConnectionProvider(cfMock).nextConnection();

        lbRule.connect(co);
        Mockito.verify(cfMock).newConnection(hosts.get(0).getValue());
        Mockito.verifyNoMoreInteractions(cfMock);

        co = loadBalancer.toConnectionProvider(cfMock).nextConnection();
        lbRule.connect(co);
        Mockito.verify(cfMock).newConnection(hosts.get(1).getValue());
        Mockito.verifyNoMoreInteractions(cfMock);
    }
}