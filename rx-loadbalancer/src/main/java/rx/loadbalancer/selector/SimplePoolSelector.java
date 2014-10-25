package rx.loadbalancer.selector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.loadbalancer.ClientEvent;
import rx.loadbalancer.ClientSelector;
import rx.loadbalancer.FailureDetector;
import rx.loadbalancer.FailureDetectorFactory;
import rx.loadbalancer.HostClientConnector;
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.util.RxUtil;
import rx.schedulers.Schedulers;

/**
 * The ClientSelector keeps track of all existing hosts and returns a single host for each
 * call to acquire().
 * 
 * @author elandau
 *
 * @param <Client>
 * 
 * TODO: Host quarantine 
 */
public class SimplePoolSelector<Host, Client> implements ClientSelector<Client>, Action1<HostEvent<Host>> {
    private static final Logger LOG = LoggerFactory.getLogger(SimplePoolSelector.class);
    
    /**
     * Track Host state
     * 
     * @author elandau
     */
    class HostHolder {
        private final Host host;
        
        private Client client;
        private boolean removed = false;
        private FailureDetector failureDetector;

        HostHolder(Host host) {
            this.host = host;
        }
        
        void setClient(Client client) {
            this.client = client;
        }
        
        void setFailureDetector(FailureDetector failureDetector) {
            this.failureDetector = failureDetector;
        }
        
        Client getClient() {
            return client;
        }
        
        void remove() {
            removed = true;
        }
        
        boolean isRemoved() {
            return removed;
        }

        Host getHost() {
            return host;
        }
        
        FailureDetector getFailureDetector() {
            return failureDetector;
        }
        
        public String toString() {
            return host.toString();
        }
    }
    
    private final Func1<HostHolder, Boolean> NOT_REMOVED = new Func1<HostHolder, Boolean>() {
        @Override
        public Boolean call(HostHolder t1) {
            return !t1.isRemoved();
        }
    };
    
    /**
     * Externally provided factory for creating a Client from a Host
     */
    private final HostClientConnector<Host, Client> connector;
    
    /**
     * Map of ALL existing hosts, connected or not
     */
    private Map<Host, HostHolder> hosts = new HashMap<Host, HostHolder>();
    
    /**
     * Queue of available hosts.  This list does not include any of the active clients
     * 
     * TODO: Change this to a semaphore and array list from which a random client can be acquired
     */
    private BlockingQueue<HostHolder> availableHosts = new LinkedBlockingQueue<HostHolder>();
    
    /**
     * Map of ALL currently acquired clients
     */
    private Map<Client, HostHolder>   acquiredClients = new IdentityHashMap<Client, HostHolder>();
    
    /**
     * Function that determines the maximum allowed pending Client creations based
     * on the total number of known hosts
     */
    private Func1<Integer, Integer> maxPendingFunc = Functions.sqrt();
    
    /**
     * Function that determines the maximum allowed Clients based on the 
     * total number of known hosts
     */
    private Func1<Integer, Integer> maxAcquiredFunc = Functions.root(4);
    
    /**
     * Number of pending Client creations.  Used to limit concurrent connects
     */
    private final AtomicInteger pendingClientCreates = new AtomicInteger();
    
    private final CopyOnWriteArrayList<Client> activeClients = new CopyOnWriteArrayList<Client>();
    
    private final FailureDetectorFactory<Host> failureDetectorFactory;
    
    public SimplePoolSelector(final HostClientConnector<Host, Client> connector, 
                              final FailureDetectorFactory<Host> failureDetectorFactory) {
        this.connector = connector;
        this.failureDetectorFactory = failureDetectorFactory;
    }
    
    @Override
    public void call(HostEvent<Host> action) {
        switch (action.getAction()) {
        case ADD:
            addHost(action.getHost());
            break;
        case REMOVE:
            removeHost(action.getHost());
            break;
        }
    }
    
    private synchronized void addHost(final Host host) {
        LOG.info("Adding host : " + host);
        
//        Preconditions.checkState(!hosts.containsKey(host));
        HostHolder holder = new HostHolder(host);
        holder.setFailureDetector(failureDetectorFactory.call(holder.host, new Action0() {
            @Override
            public void call() {
                LOG.info("Removing host");
                removeHost(host);
            }
        }));
        
        hosts.put(host, holder);
        availableHosts.add(holder);
    }
    
    private synchronized void removeHost(Host host) {
        LOG.info("Removing host : " + host);
        
        HostHolder holder = hosts.remove(host);
        if (holder != null) {
            holder.remove();
            if (holder.client != null) {
                activeClients.remove(holder.client);
                acquiredClients.remove(holder.client);
                activeClientCount--;
            }
        }
    }
    
    private boolean activateClient(final HostHolder holder, Client client) {
        LOG.info("Activate client host : " + client);
        
        holder.setClient(client);

        if (holder.removed) {
            return false;
        }
        else {
            acquiredClients.put(client, holder);
            activeClients.add(client);
            activeClientCount++;
            return true;
        }
    }
    
    private AtomicLong activeRequests = new AtomicLong();
    private volatile int activeClientCount = 0;
    
//    private final RpsEstimator rps = new RpsEstimator(1000);
//    private final AtomicReference<long[]> rpsPerHosts = Atomics.newReference(new long[0]);
            
//        State state = rps.addSample();
//        if (state != null) {
//            synchronized (this) {
//                int count = activeClients.size();
//                long[] samples = rpsPerHosts.get();
//                if (samples.length != count) {
//                    samples = Arrays.copyOf(samples, count);
//                    samples[count-1] = state.rps;
//                    if (samples.length > 2 && samples[count-1] > samples[count-2]) {
//                        connect().subscribe();
//                    }
//                }
//            }
//        }
    
    /**
     * Acquire a single client. If there are no available hosts and the max has been reached the
     * observable will not complete until a new host is available.  Make sure to call 
     * release(client) once the client is no longer in use.
     * 
     * @return Observable that will return either a single client or an empty response, where the
     *         empty response indicates that the maximum number of clients has been reached
     */
    private Observable<Client> connect() {
        LOG.info("Connect");
        
        int pendingCount = pendingClientCreates.incrementAndGet();
        int hostCount = hosts.size();
        
        // Make sure we don't try to create too many Clients
        if (acquiredClients.size() + pendingCount > maxAcquiredFunc.call(hostCount) || 
            pendingCount > this.maxPendingFunc.call(hosts.size())) {
            pendingClientCreates.decrementAndGet();
            return Observable.empty();
        }
        
        // Attempt to open one of the existing hosts but 'block' if no hosts are available
        return connectOne().finallyDo(new Action0() {
            @Override
            public void call() {
                pendingClientCreates.decrementAndGet();
            }
        });
    }
    
    private Observable<HostHolder> nextAvailable() {
        return Observable.create(new OnSubscribe<HostHolder>() {
            @Override
            public void call(Subscriber<? super HostHolder> t1) {
                try {
                    t1.onNext(availableHosts.take());
                    t1.onCompleted();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    t1.onError(e);
                }
            }
        });
    }
    
    /**
     * Try to open a connection to the next available host, retrying indefinitely
     * until a successful connection is made.
     * @return
     */
    private Observable<Client> connectOne() {
        LOG.info("ConnectOne");
        // Attempt to open one of the existing hosts but 'block' if no hosts are available
        return nextAvailable()  // TODO: Should select randomly
            .doOnNext(RxUtil.info("Trying to connect"))
            .filter(NOT_REMOVED)
            .first()
            .concatMap(new Func1<HostHolder, Observable<Client>>() {
                @Override
                public Observable<Client> call(final HostHolder holder) {
                    return connector
                        .call(holder.host, new Action1<ClientEvent>() {
                            @Override
                            public void call(ClientEvent event) {
                                LOG.info("Got event: " + event);
                                holder.getFailureDetector().call(event);
                                
                                switch (event.getType()) {
                                case REMOVED:
                                    removeHost(holder.host);
                                    break;
                                case REQUEST_START:
                                    activeRequests.incrementAndGet();
                                    break;
                                case REQUEST_SUCCESS:
                                case REQUEST_FAILURE:
                                    activeRequests.decrementAndGet();
                                    break;
                                default:
                                    break;
                                }
                            }
                        })
                        .doOnError(RxUtil.error("Failed to connect to : " + holder))
                        .filter(new Func1<Client, Boolean>() {
                            @Override
                            public Boolean call(Client client) {
                                return activateClient(holder, client);
                            }
                        });
                }
            })
            .first()
            .retry(new Func2<Integer, Throwable, Boolean>() {
                @Override
                public Boolean call(Integer t1, Throwable e) {
                    LOG.error("Got an error trying to open a connection:", e.getMessage());
                    if (e instanceof InterruptedException || e instanceof NullPointerException) {
                        LOG.error("Connectus interruptus", e);
                        return false;
                    }
                    return true;
                }
            })
            .subscribeOn(Schedulers.newThread());
    }

    @Override
    public Observable<List<Client>> aquire() {
        return Observable.create(new OnSubscribe<List<Client>>() {
            @Override
            public void call(final Subscriber<? super List<Client>> child) {
                // If no active clients then try to connect one client
                if (activeClients.isEmpty()) {
                    LOG.info("Acquire empty");
                    connect()
                        .finallyDo(new Action0() {
                            @Override
                            public void call() {
                                child.onNext(new ArrayList<Client>(activeClients));
                                child.onCompleted();
                            }
                        })
                        .subscribe();
                }
                // Return the most recent list of clients
                else {
                    LOG.info("Acquire existing");
                    child.onNext(new ArrayList<Client>(activeClients));
                    child.onCompleted();
                }
            }
        });
    }
}
