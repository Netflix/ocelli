package netflix.ocelli;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.functions.Connectors;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Failures;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author elandau
 *
 * @param <T>
 */
public class FailureDetectingInstanceFactory<T> implements Func1<T, Instance<T>> {

    public static class Builder<T> {
        private Func1<Integer, Long>            quarantineDelayStrategy = Delays.fixed(10, TimeUnit.SECONDS);
        private Func1<T, Observable<Throwable>> failureDetector         = Failures.never();
        private Func1<T, Observable<Void>>      clientConnector         = Connectors.immediate();

        /**
         * Strategy used to determine the delay time in msec based on the quarantine 
         * count.  The count is incremented by one for each failure detections and reset
         * once the host is back to normal.
         */
        public Builder<T> withQuarantineStrategy(Func1<Integer, Long> quarantineDelayStrategy) {
            this.quarantineDelayStrategy = quarantineDelayStrategy;
            return this;
        }
        
        /**
         * The failure detector returns an Observable that will emit a Throwable for each 
         * failure of the client.  The load balancer will quarantine the client in response.
         * @param failureDetector
         */
        public Builder<T> withFailureDetector(Func1<T, Observable<Throwable>> failureDetector) {
            this.failureDetector = failureDetector;
            return this;
        }
        
        /**
         * The connector can be used to prime a client prior to activating it in the connection
         * pool.  
         * @param clientConnector
         */
        public Builder<T> withClientConnector(Func1<T, Observable<Void>> clientConnector) {
            this.clientConnector = clientConnector;
            return this;
        }
        
        public FailureDetectingInstanceFactory<T> build() {
            return new FailureDetectingInstanceFactory<T>(
                    clientConnector, 
                    failureDetector, 
                    quarantineDelayStrategy);
        }
    }
        
    public static <C> Builder<C> builder() {
        return new Builder<C>();
    }
    
    private final Func1<T, Observable<Throwable>> failureDetector;
    private final Func1<T, Observable<Void>>      clientConnector;
    private final Func1<Integer, Long>            quarantineDelayStrategy;

    public FailureDetectingInstanceFactory(
            Func1<T, Observable<Void>>      clientConnector, 
            Func1<T, Observable<Throwable>> failureDetector, 
            Func1<Integer, Long>            quarantineDelayStrategy) {
        this.quarantineDelayStrategy = quarantineDelayStrategy;
        this.failureDetector         = failureDetector;
        this.clientConnector         = clientConnector;
    }
    
    @Override
    public Instance<T> call(final T client) {
        return new FailureDetectingInstance<T>(
                client, 
                clientConnector, 
                failureDetector.call(client), 
                quarantineDelayStrategy);
    }

    public String toString() {
        return "FailureDetectingInstanceFactory[]";
    }
}
