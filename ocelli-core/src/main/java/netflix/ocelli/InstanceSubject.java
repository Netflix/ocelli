package netflix.ocelli;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

/**
 * @author elandau
 */
public class InstanceSubject<T> extends Observable<Instance<T>> {

    private PublishSubject<Instance<T>> subject = PublishSubject.create();
    private ConcurrentMap<T, MutableInstance<T>> instances = new ConcurrentHashMap<T, MutableInstance<T>>();
    
    public static <T> InstanceSubject<T> create() {
        return new InstanceSubject<T>();
    }
    
    public InstanceSubject() {
        this(PublishSubject.<Instance<T>>create());
    }
    
    private InstanceSubject(final PublishSubject<Instance<T>> subject) {
        super(new OnSubscribe<Instance<T>>() {
            @Override
            public void call(Subscriber<? super Instance<T>> t1) {
                subject.subscribe(t1);
            }
        });
        this.subject = subject;
    }
    
    public void add(T t) {
        MutableInstance<T> member = MutableInstance.from(t);
        if (null == instances.putIfAbsent(t, member)) {
            subject.onNext(member);
        }
    }
    
    public void remove(T t) {
        MutableInstance<T> member = instances.remove(t);
        if (member != null) {
            member.close();
        }
    }

    public void clear() {
        for (MutableInstance<T> instance : instances.values()) {
            instance.close();
        }
        instances.clear();
    }
}
