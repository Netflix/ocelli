package netflix.ocelli;

import rx.Observable.OnSubscribe;

public interface ClientDiscovery<Client> extends OnSubscribe<MembershipEvent<Client>> {
}
