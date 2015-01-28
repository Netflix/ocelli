package netflix.ocelli;

import rx.Notification;
import rx.Observable;

public class ClientCollector<C> extends AbstractHostToClientCollector<C, C> {

    private final ClientLifecycleFactory<C> factory;
    
    public static <C> ClientCollector<C> create(ClientLifecycleFactory<C> factory)  {
        return new ClientCollector<C>(factory);
    }
    
    public static <C> ClientCollector<C> create() {
        return create(FailureDetectingClientLifecycleFactory.<C>builder().build());
    }
    
    public ClientCollector(ClientLifecycleFactory<C> factory) {
        this.factory = factory;
    }

    @Override
    protected Observable<Notification<C>> createClientLifeycle(C host) {
        return factory.call(host);
    }
}