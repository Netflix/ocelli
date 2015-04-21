package netflix.ocelli;

import netflix.ocelli.InstanceToNotification.InstanceNotification;
import rx.Notification;
import rx.Observable;
import rx.functions.Func1;

public class InstanceToNotification<T extends Instance<?>> implements Func1<T, Observable<InstanceNotification<T>>> {
    
    public static <T extends Instance<?>> InstanceToNotification<T> create() {
        return new InstanceToNotification<T>();
    }
    
    public enum Kind {
        OnAdd,
        OnRemove
    }
    
    public static class InstanceNotification<T extends Instance<?>> {
        private final T value;
        private final Kind kind;
        
        public InstanceNotification(T instance, Kind kind) {
            this.value = instance;
            this.kind = kind;
        }
        
        public Kind getKind() {
            return kind;
        }
        
        public T getInstance() {
            return value;
        }
        
        public String toString() {
            return "Notification[" + value + " " + kind + "]";
        }
    }
    
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
}
