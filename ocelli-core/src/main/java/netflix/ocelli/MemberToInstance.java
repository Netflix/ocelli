package netflix.ocelli;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * Mapping function with cache to convert a Member<H> to an Instance<C> with a plugable
 * instance factory.  
 * 
 * @author elandau
 *
 * @param <H>
 * @param <C>
 */
public class MemberToInstance<H, C> implements Func1<Member<H>, Instance<C>> {
    public static <H, C> MemberToInstance<H, C> from(Func2<H, Action0, Instance<C>> createFunc) {
        return new MemberToInstance<H, C>(createFunc);
    }
    
    private final Func2<H, Action0, Instance<C>> createFunc;
    private final ConcurrentMap<H, Instance<C>> instances = new ConcurrentHashMap<H, Instance<C>>();
    
    public MemberToInstance(Func2<H, Action0, Instance<C>> createFunc) {
        this.createFunc  = createFunc;
    }
    
    @Override
    public Instance<C> call(final Member<H> member) {
        Instance<C> instance = instances.get(member.getValue());
        if (instance == null) {
            instance = createFunc.call(member.getValue(), new Action0() {
                @Override
                public void call() {
                    instances.remove(member.getValue());
                }
            });
            Instance<C> existing = instances.putIfAbsent(member.getValue(), instance);
            if (existing != null) {
                instance.subscribe().unsubscribe();
                instance = existing;
            }
        }
        
        // We always return a proxy to the instance to allow for proper reference counting
        // via subscriptions to the underlying instance.
        // 
        // Note that the takeUntil() will cause the returned instance to unsubscribe when
        // the member is removed
        return Instance.from(instance.getValue(), instance.takeUntil(member));
    }
}
