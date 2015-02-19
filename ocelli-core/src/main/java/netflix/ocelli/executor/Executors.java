package netflix.ocelli.executor;

import java.util.NoSuchElementException;

import rx.Observable;

public class Executors {
    public static <I> Executor<I, I> identity(final I response) {
        return new Executor<I, I>() {
            @Override
            public Observable<I> call(I request) {
                return Observable.just(response);
            }
        };
    }
    
    public static <I, O> Executor<I, O> memoize(final O response) {
        return new Executor<I, O>() {
            @Override
            public Observable<O> call(I request) {
                return Observable.just(response);
            }
        };
    }

    public static <I, O> Executor<I, O> memoize(final Observable<O> response) {
        return new Executor<I, O>() {
            @Override
            public Observable<O> call(I request) {
                return response;
            }
        };
    }

    public static <I, O> Executor<I, O> error(final Exception exception) {
        return new Executor<I, O>() {
            @Override
            public Observable<O> call(I request) {
                return Observable.error(exception);
            }
        };
    }
    
    public static <I, O> Executor<I, O> empty() {
        return new Executor<I, O>() {
            @Override
            public Observable<O> call(I request) {
                return Observable.error(new NoSuchElementException());
            }
        };
    }
    
    public static <I, O> Executor<I, O> never() {
        return new Executor<I, O>() {
            @Override
            public Observable<O> call(I request) {
                return Observable.never();
            }
        };
    }
}
