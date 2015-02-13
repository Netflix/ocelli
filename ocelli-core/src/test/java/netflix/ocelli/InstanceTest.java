package netflix.ocelli;

import netflix.ocelli.util.RxUtil;

import org.junit.Test;

public class InstanceTest {
    @Test
    public void test() {
        MutableInstance<Integer> instance = MutableInstance.from(1);
        
//        FailureDetectingInstance<Integer> instance2 = new FailureDetectingInstance();
        
        instance
            .subscribe(RxUtil.info("State"), RxUtil.error("Failed"), RxUtil.info("Removed"));
        
        instance.setState(true);
        instance.setState(false);
        instance.close();
    }
}
