package netflix.ocelli.execute;

import rx.Observable;
import rx.functions.Func1;

/**
 * Higher level API for calling the load balancer as a mapping of 
 * Request -> Observable(Response).  Note that this abstraction completely 
 * removed knowledge of the client type and only deals with request/response
 *  
 * @author elandau
 *
 * @param <C>
 */
public interface ExecutionStrategy<I, O> extends Func1<I, Observable<O>>{
}
