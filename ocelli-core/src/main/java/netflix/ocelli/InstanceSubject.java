package netflix.ocelli;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

/**
 * @author elandau
 */
public class InstanceSubject<T> extends Observable<Instance<T>> {

    private final PublishSubject<Instance<T>> subject;
    private final ConcurrentMap<T, MutableInstance<T>> instances;
    
    public static <T> InstanceSubject<T> create() {
        return new InstanceSubject<T>();
    }
    
    public InstanceSubject() {
        this(PublishSubject.<Instance<T>>create(), new ConcurrentHashMap<T, MutableInstance<T>>());
    }
    
    private InstanceSubject(final PublishSubject<Instance<T>> subject, final ConcurrentMap<T, MutableInstance<T>> instances) {
        super(new OnSubscribe<Instance<T>>() {
            @Override
            public void call(Subscriber<? super Instance<T>> s) {
                Observable
                    .from(new ArrayList<Instance<T>>(instances.values()))
                    .concatWith(subject).subscribe(s);
            }
        });
        this.subject = subject;
        this.instances = instances;
    }
    
    public MutableInstance<T> add(T t) {
        MutableInstance<T> member = MutableInstance.from(t);
        MutableInstance<T> existing = instances.putIfAbsent(t, member);
        if (null == existing) {
            subject.onNext(member);
            return member;
        }
        return existing;
    }
    
    public MutableInstance<T> remove(T t) {
        MutableInstance<T> member = instances.remove(t);
        if (member != null) {
            member.close();
        }
        return member;
    }

    public void clear() {
        for (MutableInstance<T> instance : instances.values()) {
            instance.close();
        }
        instances.clear();
    }
}
