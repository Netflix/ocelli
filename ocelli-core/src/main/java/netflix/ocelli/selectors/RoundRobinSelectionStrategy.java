package netflix.ocelli.selectors;

import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.SelectionStrategy;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

/**
 * Very simple LoadBlancer that when subscribed to gets an ImmutableList of active clients 
 * and round robins on the elements in that list
 * 
 * @author elandau
 *
 * @param <Client>
 */
public class RoundRobinSelectionStrategy<C> implements SelectionStrategy<C> {
    private final AtomicInteger position = new AtomicInteger();
    
    @SuppressWarnings("unchecked")
    private volatile C[] clients = (C[]) new Object[]{};
    
    @Override
    public Observable<C> call() {
        return Observable.create(new OnSubscribe<C>() {
            @Override
            public void call(Subscriber<? super C> s) {
                C[] internal = clients;
                if (internal.length > 0) {
                    int pos = position.incrementAndGet();
                    if (pos < 0) {
                        pos = -pos;
                    }
                    s.onNext(internal[pos % internal.length]);
                }                
                s.onCompleted();
            }
        });
    }

    @Override
    public void setClients(C[] clients) {
        this.clients = clients;
    }
}
