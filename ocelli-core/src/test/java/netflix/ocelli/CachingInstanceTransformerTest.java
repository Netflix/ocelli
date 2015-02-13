package netflix.ocelli;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;
import netflix.ocelli.util.RxUtil;

import org.junit.Test;

import rx.subjects.PublishSubject;

public class CachingInstanceTransformerTest {
    @Test
    public void testMultipleHost() {
        MutableInstance<Integer> i1 = MutableInstance.from(1);
        MutableInstance<Integer> i2 = MutableInstance.from(1);
        
        PublishSubject<Instance<Integer>> s1 = PublishSubject.create();
        PublishSubject<Instance<Integer>> s2 = PublishSubject.create();
        
        final AtomicReference<List<String>> l1 = new AtomicReference<List<String>>();
        final AtomicReference<List<String>> l2 = new AtomicReference<List<String>>();
        
        CachingInstanceTransformer<Integer, String> transformer = new IntegerToStringTransformer();
        
        s1
            .map(transformer)
            .compose(new InstanceCollector<String>())
            .subscribe(RxUtil.set(l1));
        
        s2  .map(transformer)
            .compose(new InstanceCollector<String>())
            .subscribe(RxUtil.set(l2));
        
        s1.onNext(i1);
        s2.onNext(i2);

        System.out.println("L1: " + l1.get());
        System.out.println("L2: " + l2.get());
        
        System.out.println("\n>>>>>>> Closing 1\n");
        System.out.println("");
        
        i1.close();
        
        Assert.assertEquals(0, l1.get().size());
        Assert.assertEquals(1, l2.get().size());
        
        System.out.println("L1: " + l1.get());
        System.out.println("L2: " + l2.get());
        
        System.out.println("\n>>>>>>> Closing 2\n");
        
        i2.close();

        Assert.assertEquals(0, l1.get().size());
        Assert.assertEquals(0, l2.get().size());
        
        System.out.println("L1: " + l1.get());
        System.out.println("L2: " + l2.get());
    }
}
