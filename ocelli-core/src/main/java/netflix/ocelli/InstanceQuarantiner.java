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
public class InstanceQuarantiner<T extends Instance<?>> implements Func1<T, Observable<T>> {
    private final Func1<T, Observable<T>> factory;
    private final Action1<T> shutdown;
    
    /**
     * Create a new InstanceQuaratiner
     * 
     * @param failureActionSetter Function to call to associate the failure callback with a T
     */
    public static <T extends Instance<?>> InstanceQuarantiner<T> create(
            Func1<T, Observable<T>> factory,
            Action1<T> shutdown) {
        return new InstanceQuarantiner<T>(factory, shutdown);
    }
    
    public static <T extends Instance<?>> InstanceQuarantiner<T> create(
            Func1<T, Observable<T>> factory) {
        return new InstanceQuarantiner<T>(factory, null);
    }
    
    public InstanceQuarantiner(Func1<T, Observable<T>> factory, Action1<T> shutdown) {
        this.factory = factory;
        this.shutdown = shutdown;
    }
    
    @Override
    public Observable<T> call(final T primaryInstance) {
        return Observable.create(new OnSubscribe<T>() {
            @Override
            public void call(final Subscriber<? super T> s) {
                s.add(Observable
                    .defer(new Func0<Observable<T>>() {
                        @Override
                        public Observable<T> call() {
                            return factory.call(primaryInstance);
                        }
                    })
                    .switchMap(new Func1<T, Observable<Void>>() {
                        @Override
                        public Observable<Void> call(final T instance) {
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