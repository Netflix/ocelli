package netflix.ocelli.eureka;

import com.netflix.rx.eureka.client.Eureka;
import com.netflix.rx.eureka.client.EurekaClient;
import com.netflix.rx.eureka.client.resolver.ServerResolver;
import com.netflix.rx.eureka.interests.ChangeNotification;
import com.netflix.rx.eureka.interests.Interest;
import com.netflix.rx.eureka.interests.Interests;
import com.netflix.rx.eureka.registry.InstanceInfo;
import com.netflix.rx.eureka.registry.ServicePort;
import netflix.ocelli.MembershipEvent;
import rx.Observable;
import rx.functions.Func1;

import java.util.HashSet;

import static netflix.ocelli.MembershipEvent.EventType.ADD;
import static netflix.ocelli.MembershipEvent.EventType.REMOVE;

/**
 * @author Nitesh Kant
 */
public class EurekaMembershipSource {

    private final EurekaClient client;
    private static final DefaultMapper defaultMapper = new DefaultMapper();

    public EurekaMembershipSource(ServerResolver eurekaResolver) {
        this.client = Eureka.newClient(eurekaResolver);
    }

    public EurekaMembershipSource(EurekaClient client) {
        this.client = client;
    }

    public Observable<MembershipEvent<Host>> forVip(String... vips) {
        return forInterest(Interests.forVips(vips));
    }

    public Observable<MembershipEvent<Host>> forInterest(Interest<InstanceInfo> interest) {
        return forInterest(interest, defaultMapper);
    }

    public Observable<MembershipEvent<Host>> forInterest(Interest<InstanceInfo> interest,
                                                         final Func1<InstanceInfo, Host> instanceInfoToHost) {
        return client.forInterest(interest)
                     .flatMap(new Func1<ChangeNotification<InstanceInfo>, Observable<MembershipEvent<Host>>>() {
                         @Override
                         public Observable<MembershipEvent<Host>> call(ChangeNotification<InstanceInfo> notification) {
                             Host host = instanceInfoToHost.call(notification.getData());
                             switch (notification.getKind()) {
                                 case Add:
                                     return Observable.just(new MembershipEvent<>(ADD, host));
                                 case Delete:
                                     return Observable.just(new MembershipEvent<>(REMOVE, host));
                                 case Modify:
                                     return Observable.just(new MembershipEvent<>(REMOVE, host),
                                                            new MembershipEvent<>(ADD, host));
                                 default:
                                     return Observable.empty();
                             }
                         }
                     });
    }

    /**
     * A host emitted by {@link EurekaMembershipSource} which extracts required information from {@link InstanceInfo}
     */
    public static class Host {

        private String hostname;
        private int port;

        public Host(String hostname, int port) {
            if (null == hostname) {
                throw new IllegalArgumentException("Host name can not be null");
            }
            this.hostname = hostname;
            this.port = port;
        }

        public String getHostname() {
            return hostname;
        }

        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Host)) {
                return false;
            }

            Host host = (Host) o;

            if (port != host.port) {
                return false;
            }
            if (hostname != null ? !hostname.equals(host.hostname) : host.hostname != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = hostname != null ? hostname.hashCode() : 0;
            result = 31 * result + port;
            return result;
        }
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
