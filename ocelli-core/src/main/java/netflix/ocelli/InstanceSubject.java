package netflix.ocelli;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

/**
 * Container for active members of a server pool that can be manually manipulated via
 * add/remove methods.  
 * 
 * @author elandau
 */
public class InstanceSubject<T> extends Observable<Instance<T>> {

    private final PublishSubject<Instance<T>> subject;
    private final ConcurrentMap<T, CloseableInstance<T>> instances;
    
    public static <T> InstanceSubject<T> create() {
        return new InstanceSubject<T>();
    }
    
    public InstanceSubject() {
        this(PublishSubject.<Instance<T>>create(), new ConcurrentHashMap<T, CloseableInstance<T>>());
    }
    
    private InstanceSubject(final PublishSubject<Instance<T>> subject, final ConcurrentMap<T, CloseableInstance<T>> instances) {
        super(new OnSubscribe<Instance<T>>() {
            @Override
            public void call(Subscriber<? super Instance<T>> s) {
                // TODO: This is a very naive implementation that may have race conditions
                // whereby instances may be dropped
                Observable
                    .from(new ArrayList<Instance<T>>(instances.values()))
                    .concatWith(subject).subscribe(s);
            }
        });
        this.subject = subject;
        this.instances = instances;
    }
    
    public CloseableInstance<T> add(T t) {
        CloseableInstance<T> member = CloseableInstance.from(t);
        CloseableInstance<T> existing = instances.putIfAbsent(t, member);
        if (null == existing) {
            subject.onNext(member);
            return member;
        }
        return existing;
    }
    
    public CloseableInstance<T> remove(T t) {
        CloseableInstance<T> member = instances.remove(t);
        if (member != null) {
            member.close();
        }
        return member;
    }
}
