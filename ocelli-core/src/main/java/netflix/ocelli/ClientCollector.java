package netflix.ocelli;

import java.util.concurrent.CopyOnWriteArrayList;

import rx.functions.Func1;

/**
 * Collect all client into an immutable list of clients based on ADD and REMOVE
 * events
 * 
 * @author elandau
 *
 * @param <C>
 */
public class ClientCollector<C> implements Func1<MembershipEvent<C>, C[]> {
    private CopyOnWriteArrayList<C> clients = new CopyOnWriteArrayList<C>();
    
    @SuppressWarnings("unchecked")
    @Override
    public C[] call(MembershipEvent<C> event) {
        switch (event.getType()) {
        case ADD:
            clients.add(event.getClient());
            break;
        case REMOVE:
            clients.remove(event.getClient());
            break;
        default:
            break;
        }
        
        return (C[]) clients.toArray();
    }
}
