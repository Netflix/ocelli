package netflix.ocelli.selectors;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.SelectionStrategy;
import rx.Subscriber;

/**
 * Very simple LoadBlancer that when subscribed to gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class RoundRobinSelector<C> extends SelectionStrategy<C> {
    private final AtomicReference<C[]> clients;

    @SuppressWarnings("unchecked")
    public RoundRobinSelector() {
        this(new AtomicReference<C[]>((C[]) new Object[0]), new AtomicInteger());
    }
    
    RoundRobinSelector(final AtomicReference<C[]> clients, final AtomicInteger position) {
        super(new OnSubscribe<C>() {
            @Override
            public void call(Subscriber<? super C> s) {
                C[] internal = clients.get();
                if (internal.length > 0) {
                    int pos = position.incrementAndGet();
                    if (pos < 0) {
                        pos = -pos;
                    }
                    s.onNext(internal[pos % internal.length]);
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
