package netflix.ocelli.client;

import java.util.concurrent.ConcurrentMap;

import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import com.google.common.collect.Maps;

public class ManualFailureDetector implements Func1<TestClient, Observable<Throwable>> {
    private ConcurrentMap<TestClient, PublishSubject<Throwable>> clients = Maps.newConcurrentMap();
    
    @Override
    public Observable<Throwable> call(TestClient client) {
        PublishSubject<Throwable> subject = PublishSubject.create();
        PublishSubject<Throwable> prev = clients.putIfAbsent(client, subject);
        if (prev != null) {
            subject = prev;
        }
        return subject;
    }
    
    public PublishSubject<Throwable> get(TestClient client) {
        return clients.get(client);
    }

}
