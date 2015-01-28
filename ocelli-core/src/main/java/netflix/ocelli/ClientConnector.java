package netflix.ocelli;

import rx.Observable;
import rx.functions.Func1;

/**
 * Contract for priming a client prior to placing it in the connection 
 * pool.  A subscription to the client connector is made for each client 
 * instance in a load balancer.  The Observable will emit back the client
 * every time the client is considered connected. 
 *  
 * @author elandau
 *
 * @param <C>
 */
public interface ClientConnector<C> extends Func1<C, Observable<C>>{
}
