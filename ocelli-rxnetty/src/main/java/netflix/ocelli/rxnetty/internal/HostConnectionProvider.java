package netflix.ocelli.rxnetty.internal;

import io.reactivex.netty.client.ConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;

import java.util.Collection;

public class HostConnectionProvider<W, R> {

    private final ConnectionProvider<W, R> provider;
    private final TcpClientEventListener eventsListener;

    private HostConnectionProvider(ConnectionProvider<W, R> provider) {
        this(provider, null);
    }

    HostConnectionProvider(ConnectionProvider<W, R> provider, TcpClientEventListener eventsListener) {
        this.provider = provider;
        this.eventsListener = eventsListener;
    }

    public static <W, R> boolean removeFrom(Collection<HostConnectionProvider<W, R>> c,
                                            ConnectionProvider<W, R> toRemove) {
        return c.remove(new HostConnectionProvider<W, R>(toRemove));
    }

    public ConnectionProvider<W, R> getProvider() {
        return provider;
    }

    public TcpClientEventListener getEventsListener() {
        return eventsListener;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HostConnectionProvider)) {
            return false;
        }

        @SuppressWarnings("unchecked")
        HostConnectionProvider<W, R> that = (HostConnectionProvider<W, R>) o;

        if (provider != null? !provider.equals(that.provider) : that.provider != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return provider != null? provider.hashCode() : 0;
    }
}
