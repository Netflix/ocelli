package netflix.ocelli.rxnetty.internal;

import io.reactivex.netty.protocol.tcp.client.ConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.Instance;
import netflix.ocelli.rxnetty.FailureListener;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.Collections;
import java.util.List;

class HostHolder<W, R> {

    private final Observable<List<HostConnectionProvider<W, R>>> providerStream;
    private volatile List<HostConnectionProvider<W, R>> providers;
    private Subscription streamSubscription;

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
        return Observable.create(new OnSubscribe<Void>() {
            @Override
            public void call(final Subscriber<? super Void> subscriber) {
                streamSubscription = providerStream.subscribe(new Action1<List<HostConnectionProvider<W, R>>>() {
                    @Override
                    public void call(List<HostConnectionProvider<W, R>> hostConnectionProviders) {
                        /*First onNext will complete & unsubscribe this subscriber, hence this will send onComplete
                        just once*/
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onCompleted();
                        }
                        providers = hostConnectionProviders;
                    }
                }, new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onError(throwable);
                        }
                    }
                });
            }
        });
    }

    public Observable<Void> shutdown() {
        return Observable.create(new OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> subscriber) {
                if (null != streamSubscription) {
                    streamSubscription.unsubscribe();
                }

                subscriber.onCompleted();
            }
        });
    }
}
