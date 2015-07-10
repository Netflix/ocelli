package netflix.ocelli.eureka2;

import com.netflix.eureka2.client.EurekaInterestClient;
import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.interests.Interest;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.registry.datacenter.BasicDataCenterInfo;
import com.netflix.eureka2.registry.instance.InstanceInfo;
import com.netflix.eureka2.registry.instance.ServicePort;
import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import rx.Observable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class Eureka2InterestManagerTest {
    @Mock
    private EurekaInterestClient clientMock;
    private Eureka2InterestManager membershipSource;

    public static final InstanceInfo INSTANCE_1 = new InstanceInfo.Builder()
            .withId("id_serviceA")
            .withApp("ServiceA")
            .withAppGroup("ServiceA_1")
            .withStatus(InstanceInfo.Status.UP)
            .withPorts(new HashSet<ServicePort>(Collections.singletonList(new ServicePort(8000, false))))
            .withDataCenterInfo(BasicDataCenterInfo.fromSystemData())
            .build();

    public static final InstanceInfo INSTANCE_2 = new InstanceInfo.Builder()
            .withId("id_serviceA_2")
            .withApp("ServiceA")
            .withAppGroup("ServiceA_1")
            .withStatus(InstanceInfo.Status.UP)
            .withPorts(new HashSet<ServicePort>(Collections.singletonList(new ServicePort(8001, false))))
            .withDataCenterInfo(BasicDataCenterInfo.fromSystemData())
            .build();

    public static final ChangeNotification<InstanceInfo> ADD_INSTANCE_1 =
            new ChangeNotification<InstanceInfo>(ChangeNotification.Kind.Add, INSTANCE_1);

    public static final ChangeNotification<InstanceInfo> ADD_INSTANCE_2 =
            new ChangeNotification<InstanceInfo>(ChangeNotification.Kind.Add, INSTANCE_2);

    @Before
    public void setUp() throws Exception {
        membershipSource = new Eureka2InterestManager(clientMock);
    }

    @Test
    public void testVipBasedInterest() throws Exception {
        Interest<InstanceInfo> interest = Interests.forVips("test-vip");
        Mockito.when(clientMock.forInterest(interest)).thenReturn(Observable.just(ADD_INSTANCE_1, ADD_INSTANCE_2));
        
        List<Instance<Host>> instances = membershipSource
            .forInterest(interest)
            .take(2)
            .toList().toBlocking()
            .toFuture()
            .get(1, TimeUnit.SECONDS);
        
        Assert.assertEquals(2, instances.size());
        System.out.println("instances = " + instances);
    }
}
