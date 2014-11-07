package netflix.ocelli;

import rx.functions.Func1;

/**
 * Policy for determining if an error is retryable.  A retryable error implies
 * a connection error such as a timeout or lost connection.  A non-retryable error
 * indicates an application error such as bad request.
 * 
 * @author elandau
 */
public interface RetryableErrorPolicy extends Func1<Throwable, Boolean> {

}
