package netflix.ocelli;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * From a list of Instance<T> maintain a List of active T.  Add when T is up and remove
 * when T is either down or Instance failed or completed.
 * 
 * @author elandau
 *
 * @param <T>
 * 
 * TODO:  Use scan()
 */
public class InstanceCollector<T> implements Transformer<Instance<T>, List<T>> {
    
    public static <T> InstanceCollector<T> create() {
        return new InstanceCollector<T>();
    }

    @Override
    public Observable<List<T>> call(Observable<Instance<T>> o) {
        final Set<T> instances = new HashSet<T>();
        
        return o.flatMap(new Func1<Instance<T>, Observable<List<T>>>() {
            @Override
            public Observable<List<T>> call(final Instance<T> instance) {
                return instance.flatMap(
                    new Func1<Boolean, Observable<List<T>>>() {
                        @Override
                        public Observable<List<T>> call(Boolean isUp) {
                            if (isUp) {
                                if (instances.add(instance.getValue())) {
                                    return Observable.<List<T>>just(new ArrayList<T>(instances));
                                }
                            }
                            else {
                                if (instances.remove(instance.getValue())) {
                                    return Observable.<List<T>>just(new ArrayList<T>(instances));
                                }
                            }
                            return Observable.empty();
                        }
                    },
                    new Func1<Throwable, Observable<List<T>>>() {
                        @Override
                        public Observable<List<T>> call(Throwable t1) {
                            if (instances.remove(instance.getValue())) {
                                return Observable.<List<T>>just(new ArrayList<T>(instances));
                            }
                            return Observable.empty();
                        }
                    },
                    new Func0<Observable<List<T>>>() {
                        @Override
                        public Observable<List<T>> call() {
                            if (instances.remove(instance.getValue())) {
                                return Observable.<List<T>>just(new ArrayList<T>(instances));
                            }
                            return Observable.empty();
                        }
                    });
            }
        });
    }

}
