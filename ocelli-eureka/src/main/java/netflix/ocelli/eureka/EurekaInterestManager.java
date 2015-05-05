package netflix.ocelli.eureka;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import netflix.ocelli.Instance;
import netflix.ocelli.SnapshotToInstance;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;

/**
 * Wrapper for v1 DisoveryClient which offers a convenient DSL to express interests 
 * in host streams.
 * 
 * {@code
 * <pre>
 *    RoundRobinLoadBalancer lb = RoundRobinLoadBalancer.create();
 *    
 *    EurekaInterestManager manager = new EurekaInterestMangaer(discoveryClient);
 *    Subscription sub = manager
 *          .newInterest()
 *              .forApplication("applicationName")
 *              .withRefreshInterval(30, TimeUnit.SECONDS)
 *              .withScheduler(scheduler)
 *              .asObservable()
 *          .compose(InstanceCollector.<InstanceInfo>create())
 *          .subscribe(lb);
 *    
 *    lb.flatMap(operation);
 * </pre>
 * }
 * 
 * @author elandau
 */
public class EurekaInterestManager {
    private static final int DEFAULT_REFRESH_RATE = 30;
    
    private final DiscoveryClient client;
    
    private static Func1<InstanceInfo, Object> INSTANCE_TO_ID = new Func1<InstanceInfo, Object>() {
        @Override
        public Object call(InstanceInfo ii) {
            return ii.getId();
        }
    };
    
    @Inject
    public EurekaInterestManager(DiscoveryClient client) {
        this.client = client;
    }
    
    public InterestDsl newInterest() {
        return new InterestDsl(client);
    }
    
    /**
     * DSL to simplify specifying the interest 
     * 
     * @author elandau
     */
    public static class InterestDsl {
        private final DiscoveryClient client;
        
        private String   appName;
        private String   vip;
        private boolean  secure = false;
        private String   region;
        private long     interval = DEFAULT_REFRESH_RATE;
        private TimeUnit intervalUnits = TimeUnit.SECONDS;
        private Scheduler scheduler = Schedulers.computation();

        private InterestDsl(DiscoveryClient client) {
            this.client = client;
        }
        
        public InterestDsl withScheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }
        
        public InterestDsl withRefreshInterval(long interval, TimeUnit units) {
            this.interval = interval;
            this.intervalUnits = units;
            return this;
        }
        
        public InterestDsl forApplication(String appName) {
            this.appName = appName;
            return this;
        }
        
        public InterestDsl forVip(String vip) {
            this.vip = vip;
            return this;
        }
        
        public InterestDsl forRegion(String region) {
            this.region = region;
            return this;
        }
        
        public InterestDsl isSecure(boolean secure) {
            this.secure = secure;
            return this;
        }
        
        public Observable<Instance<InstanceInfo>> asObservable()  {
            return create(createLister());
        }
        
        private Observable<Instance<InstanceInfo>> create(final Func0<List<InstanceInfo>> lister) {
            return Observable
                    .interval(interval, intervalUnits, scheduler)
                    .debounce(interval/2, intervalUnits, scheduler)
                    .flatMap(new Func1<Long, Observable<List<InstanceInfo>>>() {
                        @Override
                        public Observable<List<InstanceInfo>> call(Long t1) {
                            try {
                                return Observable.just(lister.call());
                            }
                            catch (Exception e) {
                                return Observable.empty();
                            }
                        }
                    })
                    .compose(new SnapshotToInstance<InstanceInfo>(INSTANCE_TO_ID));
        }
        
        private Func0<List<InstanceInfo>> createLister() {
            if (appName != null) {
                if (vip != null) {
                    return _forVipAndApplication(vip, appName, secure);
                }
                else {
                    if (region != null) {
                        return _forApplicationAndRegion(appName, region);
                    }
                    else {
                        return _forApplication(appName);
                    }
                }
            }
            else if (vip != null) {
                if (region != null) {
                    return _forVip(vip, secure);
                }
                else {
                    return _forVipAndRegion(vip, secure, region);
                }
            }
            
            throw new IllegalArgumentException("Interest combination not supported");
        }
        
        private Func0<List<InstanceInfo>> _forApplication(final String appName) {
            return new Func0<List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call() {
                    return client.getApplication(appName).getInstances();
                }
            };
        }

        private Func0<List<InstanceInfo>> _forApplicationAndRegion(final String appName, final String region) {
            return new Func0<List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call() {
                    return client.getApplicationsForARegion(region).getRegisteredApplications(appName).getInstances();
                }
            };
        }

        private Func0<List<InstanceInfo>> _forVip(final String vip, final boolean secure) {
            return new Func0<List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call() {
                    return client.getInstancesByVipAddress(vip, secure);
                }
            };
        }
        
        private Func0<List<InstanceInfo>> _forVipAndRegion(final String vip, final boolean secure, final String region) {
            return new Func0<List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call() {
                    return client.getInstancesByVipAddress(vip, secure, region);
                }
            };
        }
        
        private Func0<List<InstanceInfo>> _forVipAndApplication(final String vip, final String appName, final boolean secure) {
            return new Func0<List<InstanceInfo>>() {
                @Override
                public List<InstanceInfo> call() {
                    return client.getInstancesByVipAddressAndAppName(vip, appName, secure);
                }
            };
        }
    }
}
