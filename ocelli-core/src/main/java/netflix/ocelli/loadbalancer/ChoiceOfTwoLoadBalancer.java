package netflix.ocelli.loadbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.LoadBalancer;
import rx.Subscriber;
import rx.functions.Func2;

/**
 * This selector chooses 2 random hosts and picks the host with the 'best' 
 * performance where that determination is deferred to a customizable function.
 * 
 * This implementation is based on the paper 'The Power of Two Choices in 
 * Randomized Load Balancing' http://www.eecs.harvard.edu/~michaelm/postscripts/tpds2001.pdf
 * This paper states that selecting the best of 2 random servers results in an 
 * exponential improvement over selecting a single random node (also includes
 * round robin) but that adding a third (or more) servers does not yield a significant
 * performance improvement.
 * 
 * @author elandau
 *
 * @param <C>
 */
public class ChoiceOfTwoLoadBalancer<C> extends LoadBalancer<C> {
    public static <C> ChoiceOfTwoLoadBalancer<C> create(final Func2<C, C, C> func) {
        return new ChoiceOfTwoLoadBalancer<C>(func);
    }

    private final AtomicReference<List<C>> clients;
    
    ChoiceOfTwoLoadBalancer(final Func2<C, C, C> func) {
        this(func, new AtomicReference<List<C>>(new ArrayList<C>()));
    }
    
    private ChoiceOfTwoLoadBalancer(final Func2<C, C, C> func, final AtomicReference<List<C>> clients) {
        super(new OnSubscribe<C>() {
            private final Random rand = new Random();
            
            @Override
            public void call(final Subscriber<? super C> s) {
                List<C> local = clients.get();
                if (local.isEmpty()) {
                    s.onError(new NoSuchElementException("No servers available in the load balancer"));
                }
                else if (local.size() == 1) {
                    s.onNext(local.get(0));
                    s.onCompleted();
                }                
                else if (local.size() > 1){
                    int first  = rand.nextInt(local.size());
                    int second = (rand.nextInt(local.size()-1) + first + 1) % local.size();
                    
                    s.onNext(func.call(local.get(first), local.get(second)));
                    s.onCompleted();
                }
            }
        });
        
        this.clients = clients;
    }

    @Override
    public void call(List<C> t) {
        this.clients.set(t);
    }
}
