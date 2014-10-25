package rx.loadbalancer;

import rx.Observable.OnSubscribe;

public interface HostDiscovery<Host> extends OnSubscribe<HostEvent<Host>> {
}
