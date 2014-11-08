package netflix.ocelli.eureka;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.HostEvent;
import netflix.ocelli.HostEvent.EventType;
import rx.Observable;
import rx.functions.Func1;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;

public class EurekaHostSource {

    private static final long DEFAULT_REFRESH_INTERVAL = 30000;
    
    public static class Builder {
        private DiscoveryClient client;
        private Func1<Long, List<InstanceInfo>> source;
        private long refreshInterval = DEFAULT_REFRESH_INTERVAL;
        
        public Builder withDiscoveryClient(DiscoveryClient client) {
            this.client = client;
            return this;
        }
        
        public Builder forApplication(final String appName) {
            source = new Func1<Long, List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call(Long t1) {
                    return client.getApplication(appName).getInstances();
                }
            };
            return this;
        }
        
        public Builder forVipAddress(final String vipAddress, final Boolean secure) {
            source = new Func1<Long, List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call(Long t1) {
                    return client.getInstancesByVipAddress(vipAddress, secure);
                }
            };
            return this;
        }
        
        public Builder forVipAddress(final String vipAddress, final Boolean secure, final String region) {
            source = new Func1<Long, List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call(Long t1) {
                    return client.getInstancesByVipAddress(vipAddress, secure, region);
                }
            };
            return this;
        }
        
        public Builder forVipAddress(final String vipAddress, final String appName, final Boolean secure) {
            source = new Func1<Long, List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call(Long t1) {
                    return client.getInstancesByVipAddressAndAppName(vipAddress, appName, secure);
                }
            };
            return this;
        }

        public Builder withRefreshInterval(long interval, TimeUnit units) {
            this.refreshInterval = TimeUnit.MILLISECONDS.convert(interval, units);
            return this;
        }
        
        public Observable<HostEvent<InstanceInfo>> build() {
            return new EurekaHostSource(this).asObservable();
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    private final Func1<Long, List<InstanceInfo>> source;
    private final long refreshInterval;
    
    private EurekaHostSource(Builder builder) {
        this.source = builder.source;
        this.refreshInterval = builder.refreshInterval;
    }
    
    private Observable<HostEvent<InstanceInfo>> asObservable() {
        return Observable.interval(refreshInterval, TimeUnit.MILLISECONDS)
            .map(source)
            .flatMap(new Func1<List<InstanceInfo>, Observable<HostEvent<InstanceInfo>>>() {
                private volatile Set<InstanceInfo> last;
                
                @Override
                public Observable<HostEvent<InstanceInfo>> call(List<InstanceInfo> current) {
                    if (last == null) {
                        last = Sets.newHashSet(current);
                        return Observable.from(last).map(HostEvent.<InstanceInfo>toEvent(EventType.ADD));
                    }
                    else {
                        Set<InstanceInfo> next = Sets.newHashSet(current);
                        SetView<InstanceInfo> newInstances     = Sets.difference(next, last);
                        SetView<InstanceInfo> removedInstances = Sets.difference(last, next);
                        last = next;
                        return Observable
                                    .from(removedInstances)
                                    .map(HostEvent.<InstanceInfo>toEvent(EventType.REMOVE))
                               .concatWith(Observable
                                    .from(newInstances)
                                    .map(HostEvent.<InstanceInfo>toEvent(EventType.ADD)));
                    }
                }
            });
    }
}
