package rx.loadbalancer.client;

import java.util.concurrent.CountDownLatch;

import rx.Observer;

public class ResponseObserver implements Observer<String> {
    private volatile Throwable t;
    private String response;
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
    
    public String get() throws Throwable {
        latch.await();
        if (this.t != null)
            throw this.t;
        return response;
    }
}
