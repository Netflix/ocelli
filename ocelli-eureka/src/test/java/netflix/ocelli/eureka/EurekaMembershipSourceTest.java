package netflix.ocelli.eureka;

import com.netflix.eureka2.client.EurekaClient;
import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.interests.Interest;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.registry.InstanceInfo;
import com.netflix.eureka2.registry.ServicePort;
import com.netflix.eureka2.registry.datacenter.BasicDataCenterInfo;
import netflix.ocelli.MembershipEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import rx.Observable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Nitesh Kant
 */
@RunWith(MockitoJUnitRunner.class)
public class EurekaMembershipSourceTest {

    @Mock
    private EurekaClient clientMock;
    private EurekaMembershipSource membershipSource;

    private static final HashSet<ServicePort> ports = new HashSet<ServicePort>(Arrays.asList(new ServicePort(8000, false)));

    public static final InstanceInfo INSTANCE_1 = new InstanceInfo.Builder()
            .withId("id_serviceA")
            .withApp("ServiceA")
            .withAppGroup("ServiceA_1")
            .withStatus(InstanceInfo.Status.UP)
            .withPorts(ports)
            .withDataCenterInfo(BasicDataCenterInfo.fromSystemData())
            .build();

    public static final InstanceInfo INSTANCE_2 = new InstanceInfo.Builder()
            .withId("id_serviceA_2")
            .withApp("ServiceA")
            .withAppGroup("ServiceA_1")
            .withStatus(InstanceInfo.Status.UP)
            .withPorts(ports)
            .withDataCenterInfo(BasicDataCenterInfo.fromSystemData())
            .build();

    public static final ChangeNotification<InstanceInfo> ADD_INSTANCE_1 =
            new ChangeNotification<InstanceInfo>(ChangeNotification.Kind.Add, INSTANCE_1);

    public static final ChangeNotification<InstanceInfo> ADD_INSTANCE_2 =
            new ChangeNotification<InstanceInfo>(ChangeNotification.Kind.Add, INSTANCE_2);

    @Before
    public void setUp() throws Exception {
        membershipSource = new EurekaMembershipSource(clientMock);
    }

    @Test
    public void testVipBasedInterest() throws Exception {
        Interest<InstanceInfo> interest = Interests.forVips("test-vip");
        Mockito.when(clientMock.forInterest(interest)).thenReturn(Observable.just(ADD_INSTANCE_1, ADD_INSTANCE_2));
        List<MembershipEvent<EurekaMembershipSource.Host>> instances = membershipSource.forInterest(interest)
                                                                                       .toList().toBlocking()
                                                                                       .toFuture()
                                                                                       .get(1, TimeUnit.MINUTES);
        System.out.println("instances = " + instances);
    }
}
