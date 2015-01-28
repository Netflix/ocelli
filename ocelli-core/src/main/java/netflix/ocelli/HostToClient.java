package netflix.ocelli;

import rx.functions.Func1;

/**
 * Create a client for a host description
 * 
 * @author elandau
 *
 * @param <H>
 * @param <C>
 */
public interface HostToClient<H, C> extends Func1<H, C> {
}
