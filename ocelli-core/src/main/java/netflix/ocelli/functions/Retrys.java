package netflix.ocelli.functions;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;

public abstract class Retrys {
    public static Func1<Observable<? extends Throwable>, Observable<?>> exponentialBackoff(final int maxRetrys, final long timeslice, final TimeUnit units) {
        return new Func1<Observable<? extends Throwable>, Observable<?>>() {
            @Override
            public Observable<?> call(Observable<? extends Throwable> attempts) {
                return attempts.flatMap(new Func1<Throwable, Observable<?>>() {
                    private int counter = 0;
                    
                    @Override
                    public Observable<?> call(Throwable e) {
                        if (counter++ == maxRetrys) {
                            return Observable.error(new Exception("Failed with " + (counter - 1)+ " retries", e));
                        }
                        return Observable.timer(counter, TimeUnit.SECONDS);
                    }
                });
            }
        };
    }

}
