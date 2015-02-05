package netflix.ocelli;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Action0;
import rx.functions.Func1;

/**
 * Convert a MembershipEvent stream to a stream of Observable<Member<C>> where onComplete is called 
 * on the Member when a REMOVE event is received.  This class also ensures that duplicates are 
 * discarded
 * 
 * @author elandau
 *
 */
public class MembershipEventToMember<C> implements Transformer<MembershipEvent<C>, Member<C>> {
    @Override
    public Observable<Member<C>> call(Observable<MembershipEvent<C>> o) {
        final ConcurrentMap<C, CloseableMember<C>> clients = new ConcurrentHashMap<C, CloseableMember<C>>();
        
        return o
            .flatMap(new Func1<MembershipEvent<C>, Observable<Member<C>>>() {
                @Override
                public Observable<Member<C>> call(MembershipEvent<C> t) {
                    switch (t.getType()) {
                    case ADD: {
                            CloseableMember<C> member = CloseableMember.from(t.getClient());
                            if (null == clients.putIfAbsent(t.getClient(), member)) {
                                return Observable.<Member<C>>just(member);
                            }
                            break;
                        }
                    case REMOVE: {
                            CloseableMember<C> member = clients.remove(t.getClient());
                            if (member != null) {
                                member.close();
                            }
                        }
                        break;
                    default:
                        break;
                    }
                    return Observable.<Member<C>>empty();
                }
            })
            .doOnUnsubscribe(new Action0() {
                @Override
                public void call() {
                    for (CloseableMember<C> client : clients.values()) {
                        client.close();
                    }
                    clients.clear();
                }
            });
    }
}
