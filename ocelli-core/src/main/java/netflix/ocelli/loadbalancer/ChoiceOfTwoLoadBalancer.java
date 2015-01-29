package netflix.ocelli.loadbalancer;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import rx.Observable;
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
public class ChoiceOfTwoLoadBalancer<C> extends BaseLoadBalancer<C>{
    public static <C> ChoiceOfTwoLoadBalancer<C> create(final Observable<List<C>> source, final Func2<C, C, C> func) {
        return new ChoiceOfTwoLoadBalancer<C>(source, func);
    }
    
    private final Random rand = new Random();
    private final Func2<C, C, C> func;
    
    ChoiceOfTwoLoadBalancer(final Observable<List<C>> source, final Func2<C, C, C> func) {
        super(source);
        this.func = func;
    }

    @Override
    public void call(Subscriber<? super C> s) {
        List<C> local = clients.get();
        if (local.size() == 1) {
            s.onNext(local.get(0));
            s.onCompleted();
        }                
        else if (local.size() > 1){
            int first  = rand.nextInt(local.size());
            int second = (rand.nextInt(local.size()-1) + first + 1) % local.size();
            
            s.onNext(func.call(local.get(first), local.get(second)));
            s.onCompleted();
        }
        else {
            s.onError(new NoSuchElementException("No servers available in the load balancer"));
        }
    }
}
