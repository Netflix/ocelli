package netflix.ocelli.rxnetty.protocol.http;

import io.reactivex.netty.protocol.http.client.events.HttpClientEventsListener;
import netflix.ocelli.rxnetty.protocol.WeightAware;

/**
 * An {@link HttpClientEventsListener} contract with an additional property defined by {@link WeightAware}
 */
public abstract class WeightedHttpClientListener extends HttpClientEventsListener implements WeightAware {
}
