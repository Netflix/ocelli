package rx.loadbalancer.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class RandomQueueTest {
    @Test
    public void shouldBeInitiallyEmpty() {
        RandomBlockingQueue<Integer> queue = new RandomBlockingQueue<Integer>();
        Assert.assertTrue(queue.isEmpty());
        Assert.assertNull(queue.peek());
        Assert.assertTrue(queue.isEmpty());
        try {
            queue.remove();
            Assert.fail();
        }
        catch (NoSuchElementException e) {
        }
    }
    
    @Test
    public void shouldBlockEmptyQueue() throws InterruptedException {
        RandomBlockingQueue<Integer> queue = new RandomBlockingQueue<Integer>();
        Assert.assertNull(queue.poll(100, TimeUnit.MILLISECONDS));
    }
    
    @Test
    @Ignore
    public void addRemoveAndShouldBlock() throws InterruptedException {
        RandomBlockingQueue<Integer> queue = new RandomBlockingQueue<Integer>();
        queue.add(123);
        Integer item = queue.take();
        Assert.assertEquals((Integer)123, item);
        Assert.assertNull(queue.poll(100, TimeUnit.MILLISECONDS));
    }
    
    @Test
    public void addOne() {
        RandomBlockingQueue<Integer> queue = new RandomBlockingQueue<Integer>();
        queue.add(123);
        
        Assert.assertTrue(!queue.isEmpty());
        Assert.assertEquals(1, queue.size());
        Assert.assertEquals((Integer)123, queue.peek());
        Assert.assertEquals((Integer)123, queue.poll());
        Assert.assertTrue(queue.isEmpty());
        Assert.assertEquals(0, queue.size());
        Assert.assertNull(queue.peek());
        
        try {
            queue.remove();
            Assert.fail();
        }
        catch (NoSuchElementException e) {
        }
    }
    
    @Test(timeout=1000)
    public void removeIsRandom() {
        RandomBlockingQueue<Integer> queue = new RandomBlockingQueue<Integer>();
        List<Integer> items = new ArrayList<Integer>();
        for (int i = 0; i < 100; i++) {
            items.add(i);
            queue.add(i);
        }
        
        List<Integer> actual = new ArrayList<Integer>();
        Integer item;
        while (null != (item = queue.poll())) {
            actual.add(item);
        }
        Assert.assertTrue(queue.isEmpty());
        Assert.assertEquals(100, actual.size());
        Assert.assertNotSame(items, actual);
        Collections.sort(actual);
        Assert.assertEquals(items, actual);
    }
}
