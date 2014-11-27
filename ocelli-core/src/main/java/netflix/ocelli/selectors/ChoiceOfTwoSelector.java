package netflix.ocelli.selectors;

import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.SelectionStrategy;
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
public class ChoiceOfTwoSelector<C> extends SelectionStrategy<C>{
    private final AtomicReference<C[]> clients;

    @SuppressWarnings("unchecked")
    public ChoiceOfTwoSelector(final Func2<C, C, C> func) {
        this(func, new AtomicReference<C[]>((C[]) new Object[0]), new Random());
    }
    
    ChoiceOfTwoSelector(final Func2<C, C, C> func, final AtomicReference<C[]> clients, final Random rand) {
        super(new OnSubscribe<C>() {
            @Override
            public void call(Subscriber<? super C> s) {
                C[] internal = clients.get();
                if (internal.length == 1) {
                    s.onNext(internal[0]);
                    s.onCompleted();
                }                
                else if (internal.length > 1){
                    int first  = rand.nextInt(internal.length);
                    int second = (rand.nextInt(internal.length-1) + first + 1) % internal.length;
                    
                    s.onNext(func.call(internal[first], internal[second]));
                    s.onCompleted();
                }
                else {
                    s.onError(new NoSuchElementException("No servers available in the load balancer"));
                }
            }
        });
        this.clients = clients;
    }
    
    @Override
    public void setClients(C[] clients) {
        this.clients.set(clients);
    }

}
