package netflix.ocelli.executor;

import java.util.NoSuchElementException;

import rx.Observable;

public class EmptyExecutor<I, O> implements Executor<I, O> {
    @Override
    public Observable<O> call(I request) {
        return Observable.error(new NoSuchElementException());
    }
}
