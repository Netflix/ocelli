package netflix.ocelli;

import rx.Observable;
import rx.functions.Func1;

public abstract class Instances {
    public static <T, S> Func1<Instance<T>, Instance<S>> transform(final Func1<T, S> func) {
        return new Func1<Instance<T>, Instance<S>>() {
            @Override
            public Instance<S> call(final Instance<T> primary) {
                final S s = func.call(primary.getValue());
                return new Instance<S>() {
                    @Override
                    public Observable<Void> getLifecycle() {
                        return primary.getLifecycle();
                    }

                    @Override
                    public S getValue() {
                        return s;
                    }
                    
                    @Override
                    public String toString() {
                        return "Instance[" + primary.getValue() + " -> " + getValue() + "]";
                    }
                };
            }
        };
    }
}
