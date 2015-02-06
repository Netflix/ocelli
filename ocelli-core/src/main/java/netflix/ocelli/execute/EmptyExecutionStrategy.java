package netflix.ocelli.execute;

import java.util.NoSuchElementException;

import rx.Observable;

public class EmptyExecutionStrategy<I, O> implements ExecutionStrategy<I, O> {
    @Override
    public Observable<O> call(I request) {
        return Observable.error(new NoSuchElementException());
    }
}
