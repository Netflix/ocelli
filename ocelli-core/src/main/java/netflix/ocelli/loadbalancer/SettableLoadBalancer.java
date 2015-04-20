package netflix.ocelli.loadbalancer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import netflix.ocelli.LoadBalancer;
import rx.functions.Action1;

public abstract class SettableLoadBalancer<T> extends LoadBalancer<T> implements Action1<List<T>> {

    protected final AtomicReference<List<T>> clients = new AtomicReference<List<T>>(Collections.<T>emptyList());

    @Override
    public void call(List<T> t) {
        this.clients.set(t);
    }

}
