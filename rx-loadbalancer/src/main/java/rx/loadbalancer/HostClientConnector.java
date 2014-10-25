package rx.loadbalancer;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func2;

public interface HostClientConnector<Host, Client> extends Func2<Host, Action1<ClientEvent>, Observable<Client>>{

}
