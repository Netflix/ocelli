package netflix.ocelli.executor;

import netflix.ocelli.executor.InterceptingExecutor.Interceptor;

import org.junit.Test;

import com.google.common.collect.Lists;

public class InterceptingExecutorTest {
    @Test
    public void testSuccess() {
        InterceptingExecutor<Integer, Integer> executor = new InterceptingExecutor<Integer, Integer>(
            Executors.<Integer, Integer>memoize(1), 
            Lists.<Interceptor<Integer, Integer>>newArrayList(new LoggingInterceptor<Integer, Integer>()));
        
        executor.call(1).subscribe();
    }
    
    @Test(expected=Exception.class)
    public void testError() {
        InterceptingExecutor<Integer, Integer> executor = new InterceptingExecutor<Integer, Integer>(
            Executors.<Integer, Integer>error(new Exception("Simulated error")), 
            Lists.<Interceptor<Integer, Integer>>newArrayList(new LoggingInterceptor<Integer, Integer>()));
        
        executor.call(1).subscribe();
    }
}
