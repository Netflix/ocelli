package netflix.ocelli.rxnetty;

import rx.functions.Func1;

public class RxNettyPendingRequests implements Func1<HttpClientMetrics, Integer> {
    @Override
    public Integer call(HttpClientMetrics t1) {
        return t1.getPendingRequests();
    }
}
