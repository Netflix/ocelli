package netflix.ocelli;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * Instance whose up state is dictated by a connector and failure detector.
 * 
 * @author elandau
 *
 * @param <T>
 */
public class FailureDetectingInstance<T> extends Instance<T> {
    static class State {
        private AtomicBoolean connected = new AtomicBoolean(true);
        private AtomicInteger failureCounter = new AtomicInteger(-1);
        
        static Func2<State, Throwable, State> Accum = new Func2<State, Throwable, State>() {
            @Override
            public State call(State current, Throwable error) {
                return current;
            }
        };
    }
    
    public FailureDetectingInstance(
            final T                          value, 
            final Func1<T, Observable<Void>> connector,
            final Observable<Throwable>      errorStream, 
            final Func1<Integer, Long>       backoffStrategy
            ) {
        super(value, new OnSubscribe<Boolean>() {
            
            Observable<Boolean> lifecycle = Observable
                    // Placeholder to trigger for connect attempt
                    .just(new Throwable("Not connected"))
                    .concatWith(errorStream)
                    .scan(new State(), State.Accum)
                    // Skip the first element since scan() duplicates the first State
                    .skip(1)
                    // Discard errors while trying to connect
                    .filter(new Func1<State, Boolean>() {
                        @Override
                        public Boolean call(State state) {
                            return state.connected.compareAndSet(true, false);
                        }
                    })
                    .switchMap(new Func1<State, Observable<Boolean>>() {
                        @Override
                        public Observable<Boolean> call(final State state) {
                            return Observable
                                // Carry the 'false' to the subscribers
                                .just(false)
                                // Concat with connection logic
                                .concatWith(Observable
                                        .just(state)
                                        // Connect with backoff if necessary
                                        .concatMap(new Func1<State, Observable<Void>>() {
                                            @Override
                                            public Observable<Void> call(State t1) {
                                                int counter = state.failureCounter.incrementAndGet();
                                                Observable<Void> connect = connector.call(value);
                                                if (counter > 0) {
                                                    connect = connect.delaySubscription(backoffStrategy.call(counter), TimeUnit.MILLISECONDS);
                                                }
                                                return connect;
                                            }
                                        })
                                        // Retry on all connect errors.  Backoff counter will increase for each connect attempt.
                                        .retry()
                                        // Reset state to connected
                                        .doOnCompleted(new Action0() {
                                            @Override
                                            public void call() {
                                                if (state.connected.compareAndSet(false, true)) {
                                                    state.failureCounter.set(0);
                                                }
                                            }
                                        })
                                        // Emit 'true' to indicate instance is up
                                        .cast(Boolean.class)
                                        .defaultIfEmpty(true));
                        }
                    })
                    .replay(1)
                    .refCount();
        
            @Override
            public void call(Subscriber<? super Boolean> s) {
                lifecycle.subscribe(s);
            }
        });
    }
}
