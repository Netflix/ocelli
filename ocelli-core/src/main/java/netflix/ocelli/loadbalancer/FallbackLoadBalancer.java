package netflix.ocelli.loadbalancer;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import netflix.ocelli.LoadBalancer;
import netflix.ocelli.functions.Retrys;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

/**
 * Composite load balancer that cascades through a set of load balancers until one has a client.
 * This type load balancer should be used for use cases such as DC and Vip failover.
 * 
 * In the following example the fallback load balancer will first attempt to choose a client from
 * dc1 and switch to dc2 if no clients exist in dc1.
 * 
 * {@code 
 * <pre>
 *    LoadBalancer<C> dc1 = RoundRobinLoadBalancer.create();
 *    LoadBalancer<C> dc2 = RoundRobinLoadBalancer.create();
 *    
 *    LoadBalancer<C> fallback = new FallbackLoadBalancer(Lists.newArrayList(dc1, dc2));
 *    fallback.subscribe(clientHandler);
 *    
 * </pre>
 * }
 * @author elandau
 *
 * @param <C>
 */
public class FallbackLoadBalancer<C> extends LoadBalancer<C> {

    public FallbackLoadBalancer(final List<LoadBalancer<C>> lbs, Func1<Throwable, Boolean> isRetriable) {
        super(new FallbackOnSubscribe<C>(lbs, isRetriable));
    }

    public FallbackLoadBalancer(final List<LoadBalancer<C>> lbs) {
        this(lbs, Retrys.ALWAYS);
    }

    private static class FallbackOnSubscribe<C> implements OnSubscribe<C> {
        private final List<LoadBalancer<C>> lbs;
        private final Func1<Throwable, Boolean> isRetriable;
        
        FallbackOnSubscribe(List<LoadBalancer<C>> lbs, Func1<Throwable, Boolean> isRetriable) {
            this.lbs = lbs;
            this.isRetriable = isRetriable;
        }
        
        @Override
        public void call(Subscriber<? super C> s) {
            _call(lbs.iterator(), s);
        }
        
        private void _call(final Iterator<LoadBalancer<C>> iter, Subscriber<? super C> s) {
            if (iter.hasNext()) {
                iter.next().onErrorResumeNext(new Func1<Throwable, Observable<C>>() {
                    @Override
                    public Observable<C> call(Throwable t1) {
                        if (!isRetriable.call(t1)) {
                            return Observable.empty();
                        }
                        return Observable.create(new OnSubscribe<C>() {
                            @Override
                            public void call(Subscriber<? super C> t1) {
                                _call(iter, t1);
                            }
                        });
                    }
                })
                .subscribe(s);
            }
            else {
                s.onError(new NoSuchElementException());
            }
        }
    }
    
    @Override
    public void call(List<C> t1) {
        throw new UnsupportedOperationException();
    }

}
