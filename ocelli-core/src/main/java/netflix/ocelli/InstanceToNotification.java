package netflix.ocelli;

import netflix.ocelli.InstanceToNotification.InstanceNotification;
import rx.Notification;
import rx.Observable;
import rx.functions.Func1;

public class InstanceToNotification<T> implements Func1<Instance<T>, Observable<InstanceNotification<T>>> {
    
    public static <T> InstanceToNotification<T> create() {
        return new InstanceToNotification<T>();
    }
    
    public enum Kind {
        OnAdd,
        OnRemove
    }
    
    public static class InstanceNotification<T> {
        private final Instance<T> value;
        private final Kind kind;
        
        public InstanceNotification(Instance<T> instance, Kind kind) {
            this.value = instance;
            this.kind = kind;
        }
        
        public Kind getKind() {
            return kind;
        }
        
        public Instance<T> getInstance() {
            return value;
        }
        
        public String toString() {
            return "Notification[" + value + " " + kind + "]";
        }
    }
    
    @Override
    public Observable<InstanceNotification<T>> call(final Instance<T> instance) {
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
