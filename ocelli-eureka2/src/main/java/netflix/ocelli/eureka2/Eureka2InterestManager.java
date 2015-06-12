package netflix.ocelli.eureka2;

import java.util.HashSet;

import javax.inject.Inject;

import netflix.ocelli.Host;
import netflix.ocelli.Instance;
import netflix.ocelli.InstanceManager;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;

import com.netflix.eureka2.client.Eureka;
import com.netflix.eureka2.client.EurekaClient;
import com.netflix.eureka2.client.resolver.ServerResolver;
import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.interests.Interest;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.registry.InstanceInfo;
import com.netflix.eureka2.registry.ServicePort;

/**
 * @author Nitesh Kant
 */
public class Eureka2InterestManager {

    private final EurekaClient client;
    private static final DefaultMapper defaultMapper = new DefaultMapper();

    public Eureka2InterestManager(ServerResolver eurekaResolver) {
        this.client = Eureka.newClient(eurekaResolver);
    }

    @Inject
    public Eureka2InterestManager(EurekaClient client) {
        this.client = client;
    }

    public Observable<Instance<Host>> forVip(String... vips) {
        return forInterest(Interests.forVips(vips));
    }

    public Observable<Instance<Host>> forInterest(Interest<InstanceInfo> interest) {
        return forInterest(interest, defaultMapper);
    }

    public Observable<Instance<Host>> forInterest(final Interest<InstanceInfo> interest, final Func1<InstanceInfo, Host> instanceInfoToHost) {
        return Observable.create(new OnSubscribe<Instance<Host>>() {
            @Override
            public void call(Subscriber<? super Instance<Host>> s) {
                final InstanceManager<Host> subject = InstanceManager.create();
                s.add(client
                        .forInterest(interest)
                        .subscribe(new Action1<ChangeNotification<InstanceInfo>>() {
                            @Override
                            public void call(ChangeNotification<InstanceInfo> notification) {
                                Host host = instanceInfoToHost.call(notification.getData());
                                switch (notification.getKind()) {
                                    case Add:
                                        subject.add(host);
                                        break;
                                    case Delete:
                                        subject.remove(host);
                                        break;
                                    case Modify:
                                        subject.remove(host);
                                        subject.add(host);
                                        break;
                                    default:
                                        break;
                                }
                            }
                        }));
                
                subject.subscribe(s);
            }
        });
    }

    protected static class DefaultMapper implements Func1<InstanceInfo, Host> {

        @Override
        public Host call(InstanceInfo instanceInfo) {
            String ipAddress = instanceInfo.getDataCenterInfo().getDefaultAddress().getIpAddress();
            HashSet<ServicePort> servicePorts = instanceInfo.getPorts();
            ServicePort portToUse = servicePorts.iterator().next();
            return new Host(ipAddress, portToUse.getPort());
        }
    }
}
