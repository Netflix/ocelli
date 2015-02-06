package netflix.ocelli.executor;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import rx.Observable;
import rx.functions.Func1;

/**
 * Composite ExecutionStrategy which falls back through a sequence of Executors
 * as long as the error is retriable.  
 * 
 * @author elandau
 *
 */
public class FallbackExecutor<I, O> implements Executor<I, O> {
    private final List<Executor<I, O>> sequence;
    private final Func1<Throwable, Boolean> isRetriable;
    
    public static Func1<Throwable, Boolean> DEFAULT_IS_RETRIABLE = new Func1<Throwable, Boolean>() {
        @Override
        public Boolean call(Throwable t1) {
            return t1 instanceof NoSuchElementException;
        }
    };
    
    public FallbackExecutor(List<Executor<I, O>> sequence, Func1<Throwable, Boolean> isRetriable) {
        this.sequence = sequence;
        this.isRetriable = isRetriable;
    }
    
    public FallbackExecutor(List<Executor<I, O>> sequence) {
        this(sequence, DEFAULT_IS_RETRIABLE);
    }
    
    @Override
    public Observable<O> call(final I request) {
        return _call(sequence.iterator(), request);
    }

    private Observable<O> _call(Iterator<Executor<I, O>> iter, I request) {
        Observable<O> o = iter.next().call(request);
        if (iter.hasNext()) {
            o = switchOnError(o, iter, request);
        }
        return o;
    }
    private Observable<O> switchOnError(Observable<O> o, final Iterator<Executor<I, O>> iter, final I request) {
        return o.onErrorResumeNext(new Func1<Throwable, Observable<O>>() {
            @Override
            public Observable<O> call(Throwable error) {
                if (isRetriable.call(error) && iter.hasNext()) {
                    return _call(iter, request);
                }
                return Observable.error(error);
            }
        });
    }
}
