package netflix.ocelli.eureka;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;
import netflix.ocelli.InstanceCollector;
import netflix.ocelli.util.RxUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import rx.schedulers.TestScheduler;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.shared.Application;

@RunWith(MockitoJUnitRunner.class)
public class EurekaInterestManagerTest {
    @Mock
    private DiscoveryClient client;
    
    @Mock
    private Application application;
    
    @Test
    public void testAddRemoveInstances() {
        InstanceInfo i1 = createInstance(1);
        InstanceInfo i2 = createInstance(2);
        InstanceInfo i3 = createInstance(3);
        InstanceInfo i4 = createInstance(4);
        
        Mockito.when(client.getApplication("foo")).thenReturn(application);
        
        AtomicReference<List<InstanceInfo>> result = new AtomicReference<List<InstanceInfo>>();
        
        TestScheduler scheduler = new TestScheduler();
        EurekaInterestManager eureka = new EurekaInterestManager(client);
        eureka.newInterest()
            .forApplication("foo")
            .withRefreshInterval(1, TimeUnit.SECONDS)
            .withScheduler(scheduler)
            .asObservable()
            .compose(InstanceCollector.<InstanceInfo>create())
            .subscribe(RxUtil.set(result));

        Mockito.when(application.getInstances()).thenReturn(Arrays.asList(i1, i2));
        scheduler.advanceTimeBy(10, TimeUnit.SECONDS);
        Assert.assertEquals(Arrays.asList(i2, i1), result.get());
        
        Mockito.when(application.getInstances()).thenReturn(Arrays.asList(i1, i2, i3));
        scheduler.advanceTimeBy(10, TimeUnit.SECONDS);
        Assert.assertEquals(Arrays.asList(i3, i2, i1), result.get());
       
        Mockito.when(application.getInstances()).thenReturn(Arrays.asList(i3, i4));
        scheduler.advanceTimeBy(10, TimeUnit.SECONDS);
        Assert.assertEquals(Arrays.asList(i3, i4), result.get());
        
        Mockito.when(application.getInstances()).thenReturn(Arrays.<InstanceInfo>asList());
        scheduler.advanceTimeBy(10, TimeUnit.SECONDS);
        Assert.assertEquals(Arrays.asList(), result.get());
    }
    
    InstanceInfo createInstance(int id) {
        return InstanceInfo.Builder.newBuilder()
                .setHostName("localhost:800" + id)
                .setAppName("foo")
                .setStatus(InstanceStatus.UP)
                .build();
    }
}
