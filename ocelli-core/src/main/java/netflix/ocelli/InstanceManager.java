package netflix.ocelli;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

/**
 * InstanceSubject can be used as a basic bridge from an add/remove host membership
 * paradigm to Ocelli's internal Instance with lifecycle representation of entity
 * membership in the load balancer.  
 * 
 * @see LoadBalancer
 * 
 * @author elandau
 */
public class InstanceManager<T> extends Observable<Instance<T>> {

    private final PublishSubject<Instance<T>> subject;
    private final ConcurrentMap<T, CloseableInstance<T>> instances;
    
    public static <T> InstanceManager<T> create() {
        return new InstanceManager<T>();
    }
    
    public InstanceManager() {
        this(PublishSubject.<Instance<T>>create(), new ConcurrentHashMap<T, CloseableInstance<T>>());
    }
    
    private InstanceManager(final PublishSubject<Instance<T>> subject, final ConcurrentMap<T, CloseableInstance<T>> instances) {
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
    
    /**
     * Add an entity to the source, which feeds into a load balancer
     * @param t
     * @return
     */
    public CloseableInstance<T> add(T t) {
        CloseableInstance<T> member = CloseableInstance.from(t);
        CloseableInstance<T> existing = instances.putIfAbsent(t, member);
        if (null == existing) {
            subject.onNext(member);
            return member;
        }
        return existing;
    }
    
    /**
     * Remove an entity from the source.  If the entity exists it's lifecycle will
     * onComplete.
     * 
     * @param t
     * @return
     */
    public CloseableInstance<T> remove(T t) {
        CloseableInstance<T> member = instances.remove(t);
        if (member != null) {
            member.close();
        }
        return member;
    }
}
