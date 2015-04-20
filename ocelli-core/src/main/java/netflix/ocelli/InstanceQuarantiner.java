package netflix.ocelli;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
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
public class InstanceQuarantiner<T> implements Func1<Instance<T>, Observable<Instance<T>>> {
    private final Func1<T, Observable<Instance<T>>> factory;
    
    /**
     * Create a new InstanceQuaratiner
     * 
     * @param failureActionSetter Function to call to associate the failure callback with a T
     */
    public static <T> InstanceQuarantiner<T> create(Func1<T, Observable<Instance<T>>> factory) {
        return new InstanceQuarantiner<T>(factory);
    }
    
    public InstanceQuarantiner(Func1<T, Observable<Instance<T>>> factory) {
        this.factory = factory;
    }
    
    @Override
    public Observable<Instance<T>> call(final Instance<T> primaryInstance) {
        return Observable.create(new OnSubscribe<Instance<T>>() {
            @Override
            public void call(final Subscriber<? super Instance<T>> s) {
                s.add(Observable
                    .defer(new Func0<Observable<Instance<T>>>() {
                        @Override
                        public Observable<Instance<T>> call() {
                            return factory.call(primaryInstance.getValue());
                        }
                    })
                    .switchMap(new Func1<Instance<T>, Observable<Void>>() {
                        @Override
                        public Observable<Void> call(Instance<T> secondaryInstance) {
                            s.onNext(Instance.from(secondaryInstance.getValue(), secondaryInstance.getLifecycle().ambWith(primaryInstance.getLifecycle())));
                            return secondaryInstance.getLifecycle();
                        }
                    })
                    .repeat()
                    .takeUntil(primaryInstance.getLifecycle())
                    .subscribe());
            }
        });
    }
}