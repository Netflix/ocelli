package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.LoadBalancer;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.subscriptions.Subscriptions;

/**
 * Base for any {@link LoadBalancer} whose pool of available targets is tracked as an immutable
 * list of T that can be updated at any time.  A SettableLoadBalancer is meant to be the final
 * subscriber to an RxJava stream that manages lifecycle for targets.
 * 
 * @author elandau
 *
 * @param <T>
 */
public abstract class AbstractLoadBalancer<T> extends LoadBalancer<T> {

    private final static Subscription              IDLE_SUBSCRIPTION = Subscriptions.empty();
    private final static Subscription              SHUTDOWN_SUBSCRIPTION = Subscriptions.empty();
    
    private final    Observable<List<T>>           source;
    private final    AtomicBoolean                 isSourceSubscribed = new AtomicBoolean(false);
    private final    AtomicReference<Subscription> subscription       = new AtomicReference<Subscription>(IDLE_SUBSCRIPTION);
    private volatile List<T>                       cache;
    
    public AbstractLoadBalancer(Observable<List<T>> source) {
        this.source = source;
    }

    public T next() throws NoSuchElementException {
        if (isSourceSubscribed.compareAndSet(false, true)) {
            Subscription s = source.subscribe(new Action1<List<T>>() {
                @Override
                public void call(List<T> t1) {
                    cache = t1;
                }
            });
            
            if (!subscription.compareAndSet(IDLE_SUBSCRIPTION, s)) {
                s.unsubscribe();
            }
        }
        
        List<T> latest = cache;
        if (latest == null) {
            throw new NoSuchElementException();
        }
        
        return choose(latest);
    }
    
    public void shutdown() {
        Subscription s = subscription.getAndSet(SHUTDOWN_SUBSCRIPTION);
        s.unsubscribe();
        cache = null;
    }

    protected abstract T choose(List<T> instances) throws NoSuchElementException;
}
