package netflix.ocelli.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import rx.Observer;

public class ResponseObserver implements Observer<String> {
    private volatile Throwable t;
    private volatile String response;
    private CountDownLatch latch = new CountDownLatch(1);
    
    @Override
    public void onCompleted() {
        latch.countDown();
    }

    @Override
    public void onError(Throwable e) {
        this.t = e;
        latch.countDown();
    }

    @Override
    public void onNext(String t) {
        this.response = t;
    }
    
    public String await(long duration, TimeUnit units) throws Throwable {
        latch.await(duration, units);
        if (this.t != null)
            throw this.t;
        return response;
    }
    
    public String get() {
        return response;
    }
}
