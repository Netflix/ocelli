package netflix.ocelli.execute;

import java.util.List;
import java.util.NoSuchElementException;

import rx.Observable;
import rx.functions.Func1;

/**
 * Composite ExecutionStrategy which falls back through a sequence of ExecutionStrategies
 * as long as the error is retriable.  
 * 
 * @author elandau
 *
 */
public class FallbackExecutionStrategy<I, O> implements ExecutionStrategy<I, O> {
    private final List<ExecutionStrategy<I, O>> sequence;
    private final Func1<Throwable, Boolean> isRetriable;
    
    public static Func1<Throwable, Boolean> DEFAULT_IS_RETRIABLE = new Func1<Throwable, Boolean>() {
        @Override
        public Boolean call(Throwable t1) {
            return t1 instanceof NoSuchElementException;
        }
    };
    
    public FallbackExecutionStrategy(List<ExecutionStrategy<I, O>> sequence, Func1<Throwable, Boolean> isRetriable) {
        this.sequence = sequence;
        this.isRetriable = isRetriable;
    }
    
    public FallbackExecutionStrategy(List<ExecutionStrategy<I, O>> sequence) {
        this(sequence, DEFAULT_IS_RETRIABLE);
    }
    
    @Override
    public Observable<O> call(final I request) {
        Observable<O> o = null;
        for (ExecutionStrategy<I, O> executor : sequence) {
            if (o == null) {
                o = executor.call(request);
            }
            else {
                o = switchOnError(o, executor, request);
            }
        }
        return o;
    }
    
    private Observable<O> switchOnError(Observable<O> o, final ExecutionStrategy<I, O> vip, final I request) {
        return o.onErrorResumeNext(new Func1<Throwable, Observable<O>>() {
            @Override
            public Observable<O> call(Throwable error) {
                if (isRetriable.call(error)) {
                    return vip.call(request);
                }
                return Observable.error(error);
            }
        });
    }
}
