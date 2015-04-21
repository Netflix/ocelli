package netflix.ocelli;

import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * To be used as a flatMap on a stream of Instance<T> the quarantiner converts the Instance<T> 
 * to an Observable<Instance<T>> where each emitted item represents an active Instance<T> based
 * on a failure detector specific to T.  When failure is detected the emitted instance's lifecycle
 * will be terminated and a new Instance<T> based on the original Instance<T> (passed to the flatMap)
 * will be emitted after a configurable quarantine time.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class InstanceQuarantiner<I extends Instance<?>> implements Func1<I, Observable<I>> {
    private final Func1<I, Observable<I>> factory;
    private final Action1<I> shutdown;
    
    /**
     * Create a new InstanceQuaratiner
     * 
     * @param failureActionSetter Function to call to associate the failure callback with a T
     */
    public static <I extends Instance<?>> InstanceQuarantiner<I> create(
            Func1<I, Observable<I>> factory,
            Action1<I> shutdown) {
        return new InstanceQuarantiner<I>(factory, shutdown);
    }
    
    public static <I extends Instance<?>> InstanceQuarantiner<I> create(
            Func1<I, Observable<I>> factory) {
        return new InstanceQuarantiner<I>(factory, null);
    }
    
    public InstanceQuarantiner(Func1<I, Observable<I>> factory, Action1<I> shutdown) {
        this.factory = factory;
        this.shutdown = shutdown;
    }
    
    @Override
    public Observable<I> call(final I primaryInstance) {
        return Observable.create(new OnSubscribe<I>() {
            @Override
            public void call(final Subscriber<? super I> s) {
                s.add(Observable
                    .defer(new Func0<Observable<I>>() {
                        @Override
                        public Observable<I> call() {
                            return factory.call(primaryInstance);
                        }
                    })
                    .switchMap(new Func1<I, Observable<Void>>() {
                        @Override
                        public Observable<Void> call(final I instance) {
                            s.onNext(instance);
                            return instance
                                    .getLifecycle()
                                    .doOnCompleted(new Action0() {
                                        private AtomicBoolean done = new AtomicBoolean(false);
                                        @Override
                                        public void call() {
                                            if (done.compareAndSet(false, true)) {
                                                if (shutdown != null) {
                                                    shutdown.call(instance);
                                                }
                                            }
                                        }
                                    });
                        }
                    })
                    .repeat()
                    .takeUntil(primaryInstance.getLifecycle())
                    .subscribe());
            }
        });
    }
}