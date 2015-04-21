package netflix.ocelli.rxnetty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import netflix.ocelli.Instance;
import rx.Notification;
import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;
import rx.functions.Func2;

public class ServerCollector<T extends Instance<?>> implements Transformer<T, List<T>> {
    
    public static <T extends Instance<?>> ServerCollector<T> create() {
        return new ServerCollector<T>();
    }

    public enum Kind {
        OnAdd,
        OnRemove
    }
    
    public static class InstanceNotification<T> {
        private final T value;
        private final Kind kind;
        
        public InstanceNotification(T instance, Kind kind) {
            this.value = instance;
            this.kind = kind;
        }
        
        public Kind getKind() {
            return kind;
        }
        
        public T getValue() {
            return value;
        }
        
        public String toString() {
            return "Notification[" + value + " " + kind + "]";
        }
    }
    
    @Override
    public Observable<List<T>> call(Observable<T> o) {
        return o
            .flatMap(new Func1<T, Observable<InstanceNotification<T>>>() {
                @Override
                public Observable<InstanceNotification<T>> call(final T instance) {
                    return Observable
                            .just(new InstanceNotification<T>(instance, Kind.OnAdd))
                            .concatWith(instance.getLifecycle().materialize().map(new Func1<Notification<Void>, InstanceNotification<T>>() {
                                @Override
                                public InstanceNotification<T> call(Notification<Void> t1) {
                                    return new InstanceNotification<T>(instance, Kind.OnRemove);
                                }
                            }));
                }
            })
            .scan(new HashSet<T>(), new Func2<HashSet<T>, InstanceNotification<T>, HashSet<T>>() {
                @Override
                public HashSet<T> call(HashSet<T> instances, InstanceNotification<T> notification) {
                    
                    switch (notification.getKind()) {
                    case OnAdd:
                        instances.add(notification.getValue());
                        break;
                    case OnRemove:
                        instances.remove(notification.getValue());
                        break;
                    }
                    return instances;
                }
            })
            .map(new Func1<HashSet<T>, List<T>>() {
                @Override
                public List<T> call(HashSet<T> instances) {
                    // Make an immutable copy of the list
                    return Collections.unmodifiableList(new ArrayList<T>(instances));
                }
            });
    }
}
