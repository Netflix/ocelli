package netflix.ocelli;

import rx.Notification;
import rx.Observable;

public class HostToClientCollector<H, C> extends AbstractHostToClientCollector<H, C> {

    private final HostToClientLifecycleFactory<H, C> factory;
    
    public static <H, C> HostToClientCollector<H, C> create(HostToClientLifecycleFactory<H, C> factory) {
        return new HostToClientCollector<H, C>(factory);
    }
    
    public HostToClientCollector(HostToClientLifecycleFactory<H, C> factory) {
        this.factory = factory;
    }

    @Override
    protected Observable<Notification<C>> createClientLifeycle(H host) {
        return factory.call(host);
    }

}
