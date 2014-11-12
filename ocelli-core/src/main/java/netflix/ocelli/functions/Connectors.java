package netflix.ocelli.functions;

import netflix.ocelli.ClientConnector;
import rx.Observable;

public class Connectors {
    public static <C> ClientConnector<C> never() {
        return new ClientConnector<C>() {
            @Override
            public Observable<C> call(C client) {
                return Observable.never();
            }
        };
    }
    
    public static <C> ClientConnector<C> immediate() {
        return new ClientConnector<C>() {
            @Override
            public Observable<C> call(C client) {
                return Observable.just(client);
            }
        };
    }
    
    public static <C> ClientConnector<C> failure(final Throwable t) {
        return new ClientConnector<C>() {
            @Override
            public Observable<C> call(C client) {
                return Observable.error(t);
            }
        };
    }

}
