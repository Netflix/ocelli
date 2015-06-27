package netflix.ocelli.rxnetty.internal;

import io.reactivex.netty.protocol.tcp.client.ConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.Instance;
import netflix.ocelli.rxnetty.FailureListener;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class HostHolder<W, R> {

    private final Observable<List<HostConnectionProvider<W, R>>> providerStream;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile List<HostConnectionProvider<W, R>> providers;

    HostHolder(Observable<Instance<ConnectionProvider<W, R>>> providerStream,
               final Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory) {
        this.providerStream = providerStream.lift(new HostCollector<W, R>(eventListenerFactory))
                                            .serialize()/*Host collector emits concurrently*/;
        providers = Collections.emptyList();
    }

    List<HostConnectionProvider<W, R>> getProviders() {
        return providers;
    }

    public Observable<Void> start() {

        if (started.compareAndSet(false, true)) {
            providerStream.subscribe(new Action1<List<HostConnectionProvider<W, R>>>() {
                @Override
                public void call(List<HostConnectionProvider<W, R>> hostConnectionProviders) {
                    providers = hostConnectionProviders;
                }
            });
        }

        return Observable.empty();
    }
}
