package netflix.ocelli.util;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import rx.functions.Action1;

public class CountDownAction<T> implements Action1<T> {
    private CountDownLatch latch;
    private CopyOnWriteArrayList<T> list = new CopyOnWriteArrayList<T>();
    
    public CountDownAction(int count) {
        latch = new CountDownLatch(count);
    }
    
    @Override
    public void call(T t1) {
        list.add(t1);
        latch.countDown();
    }
    
    public void await(long timeout, TimeUnit units) throws Exception {
        latch.await(timeout, units);
    }
    
    public List<T> get() {
        return list;
    }
    
    public void reset(int count) {
        latch = new CountDownLatch(count);
        list = new CopyOnWriteArrayList<T>();
    }
}
