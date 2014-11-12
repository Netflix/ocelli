package netflix.ocelli;

import rx.Observable;
import rx.functions.Func1;

public interface MetricsFactory<C, M> extends Func1<C, Observable<M>> {

}
