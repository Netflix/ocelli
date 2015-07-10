package netflix.ocelli.rxnetty.protocol.tcp;

import io.reactivex.netty.protocol.http.client.events.HttpClientEventsListener;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.rxnetty.protocol.WeightAware;

/**
 * An {@link TcpClientEventListener} contract with an additional property defined by {@link WeightAware}
 */
public abstract class WeightedTcpClientListener extends HttpClientEventsListener implements WeightAware {
}
