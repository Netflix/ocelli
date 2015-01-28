package netflix.ocelli;

import rx.Notification;
import rx.Observable;
import rx.functions.Func1;

public interface ClientLifecycleFactory<C> extends Func1<C, Observable<Notification<C>>> {

}
