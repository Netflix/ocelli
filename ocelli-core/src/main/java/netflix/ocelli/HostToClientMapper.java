package netflix.ocelli;

import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.util.RxUtil;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * Convert a Host to a client instance that is attached to a failure detector
 * and calls a shutdown action once all subscriptions have been removed.
 * 
 * Note that when H and C are the same createFunc should simply return the 
 * same instance.
 * 
 * @author elandau
 *
 * @param <H>
 * @param <C>
 */
public class HostToClientMapper<H, C> implements Func2<H, Action0, Instance<C>> {
    
    private final Func1<C, Observable<Boolean>> failureDetectorFactory;
    private final Func1<H, C> createFunc;
    private final Action1<C> destroyAction;
    
    public HostToClientMapper(
            Func1<H, C> createFunc,
            Action1<C> destroyAction,
            Func1<C, Observable<Boolean>> failureDetectorFactory) {
        this.failureDetectorFactory = failureDetectorFactory;
        this.createFunc = createFunc;
        this.destroyAction = destroyAction;
    }
    
    @Override
    public Instance<C> call(final H host, final Action0 shutdown) {
        final C client = createFunc.call(host);
        final AtomicInteger refCount = new AtomicInteger();
        
        return Instance.from(client, failureDetectorFactory.call(client)
            .doOnSubscribe(RxUtil.increment(refCount))
            .doOnUnsubscribe(new Action0() {
                @Override
                public void call() {
                    if (refCount.decrementAndGet() == 0) {
                        shutdown.call();
                        destroyAction.call(client);
                    }
                }
            }));
    }
}
