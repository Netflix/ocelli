package netflix.ocelli.rxnetty.protocol.tcp;

import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.loadbalancer.ChoiceOfTwoLoadBalancer;

/**
 * An {@link TcpClientEventListener} contract which is also a {@link Comparable}. These listeners are typically used
 * with a load balancer that chooses the best among two servers, eg: {@link ChoiceOfTwoLoadBalancer}
 */
public abstract class ComparableTcpClientListener extends TcpClientEventListener
        implements Comparable<ComparableTcpClientListener> {
}
