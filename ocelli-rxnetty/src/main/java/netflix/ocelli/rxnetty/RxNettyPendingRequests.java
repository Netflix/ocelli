package netflix.ocelli.rxnetty;

import rx.functions.Func1;

public class RxNettyPendingRequests implements Func1<RxNettyHttpClientAndMetrics, Integer> {
    @Override
    public Integer call(RxNettyHttpClientAndMetrics t1) {
        return t1.getPendingRequests();
    }
}
