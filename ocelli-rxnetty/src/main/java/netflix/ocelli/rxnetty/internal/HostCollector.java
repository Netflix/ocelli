package netflix.ocelli.rxnetty.internal;

import io.reactivex.netty.protocol.tcp.client.ConnectionProvider;
import io.reactivex.netty.protocol.tcp.client.events.TcpClientEventListener;
import netflix.ocelli.Instance;
import netflix.ocelli.rxnetty.FailureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Actions;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

class HostCollector<W, R> implements Operator<List<HostConnectionProvider<W, R>>, Instance<ConnectionProvider<W, R>>> {

    private static final Logger logger = LoggerFactory.getLogger(HostCollector.class);

    protected final CopyOnWriteArrayList<HostConnectionProvider<W, R>> currentHosts = new CopyOnWriteArrayList<>();
    private final Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory;

    HostCollector(Func1<FailureListener, ? extends TcpClientEventListener> eventListenerFactory) {
        this.eventListenerFactory = eventListenerFactory;
    }

    @Override
    public Subscriber<? super Instance<ConnectionProvider<W, R>>>
    call(final Subscriber<? super List<HostConnectionProvider<W, R>>> o) {

        return new Subscriber<Instance<ConnectionProvider<W, R>>>() {
            @Override
            public void onCompleted() {
                o.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                o.onError(e);
            }

            @Override
            public void onNext(Instance<ConnectionProvider<W, R>> i) {

                final ConnectionProvider<W, R> provider = i.getValue();

                final TcpClientEventListener listener = eventListenerFactory.call(newFailureListener(provider, o));

                final HostConnectionProvider<W, R> hcp = new HostConnectionProvider<>(i.getValue(), listener);

                addHost(hcp, o);

                bindToInstanceLifecycle(i, hcp, o);
            }
        };
    }

    protected void removeHost(HostConnectionProvider<W, R> toRemove,
                              Subscriber<? super List<HostConnectionProvider<W, R>>> hostListListener) {
        /*It's a copy-on-write list, so removal makes a copy with no interference to reads*/
        currentHosts.remove(toRemove);
        hostListListener.onNext(currentHosts);
    }

    protected void addHost(HostConnectionProvider<W, R> toAdd,
                           Subscriber<? super List<HostConnectionProvider<W, R>>> hostListListener) {
        /*It's a copy-on-write list, so addition makes a copy with no interference to reads*/
        currentHosts.add(toAdd);
        hostListListener.onNext(currentHosts);
    }

    protected FailureListener newFailureListener(final ConnectionProvider<W, R> provider,
                                                 final Subscriber<? super List<HostConnectionProvider<W, R>>> hostListListener) {

        return new FailureListener() {
            @Override
            public void remove() {
                HostConnectionProvider.removeFrom(currentHosts, provider);
                hostListListener.onNext(currentHosts);
            }

            @Override
            public void quarantine(long quarantineDuration, TimeUnit timeUnit) {
                quarantine(quarantineDuration, timeUnit, Schedulers.computation());
            }

            @Override
            public void quarantine(long quarantineDuration, TimeUnit timeUnit, Scheduler timerScheduler) {
                final FailureListener fl = this;
                remove();
                Observable.timer(quarantineDuration, timeUnit, timerScheduler)
                          .subscribe(new Action1<Long>() {
                              @Override
                              public void call(Long aLong) {
                                  TcpClientEventListener listener = eventListenerFactory.call(fl);
                                  addHost(new HostConnectionProvider<W, R>(provider, listener), hostListListener);
                              }
                          }, new Action1<Throwable>() {
                              @Override
                              public void call(Throwable throwable) {
                                  logger.error("Error while adding back a quarantine instance to the load balancer.",
                                               throwable);
                              }
                          });
            }
        };
    }

    protected void bindToInstanceLifecycle(Instance<ConnectionProvider<W, R>> i,
                                           final HostConnectionProvider<W, R> hcp,
                                           final Subscriber<? super List<HostConnectionProvider<W, R>>> o) {
        i.getLifecycle()
         .finallyDo(new Action0() {
             @Override
             public void call() {
                 removeHost(hcp, o);
             }
         })
         .subscribe(Actions.empty(), new Action1<Throwable>() {
             @Override
             public void call(Throwable throwable) {
                 // Do nothing as finallyDo takes care of both complete and error.
             }
         });
    }
}
