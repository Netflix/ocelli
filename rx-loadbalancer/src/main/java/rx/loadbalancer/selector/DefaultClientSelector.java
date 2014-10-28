package rx.loadbalancer.selector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
import rx.loadbalancer.ClientTrackerFactory;
import rx.loadbalancer.HostClientConnector;
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.WeightingStrategy;
import rx.loadbalancer.algorithm.IdentityWeightingStrategy;
import rx.loadbalancer.loadbalancer.ClientsAndWeights;
import rx.loadbalancer.util.RxUtil;
import rx.schedulers.Schedulers;
import rx.subscriptions.CompositeSubscription;

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
public class DefaultClientSelector<Host, Client, Tracker extends Action1<ClientEvent>> implements ClientSelector<Client> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultClientSelector.class);
    
    /**
     * Da Builder 
     * @author elandau
     *
     * @param <Host>
     * @param <Client>
     */
    public static class Builder<Host, Client, Tracker extends Action1<ClientEvent>> {
        private Func1<Integer, Integer> maxPendingFunc = Functions.sqrt();
        private Func1<Integer, Integer> maxAcquiredFunc = Functions.root(4);
        private HostClientConnector<Host, Client> connector;
        private ClientTrackerFactory<Host, Tracker> clientTrackerFactory;
        private Observable<HostEvent<Host>> hostSource;
        private WeightingStrategy<Host, Client, Tracker> weightingStrategy = new IdentityWeightingStrategy<Host, Client, Tracker>();
        
        public Builder<Host, Client, Tracker> withMaxPendingStrategy(Func1<Integer, Integer> func) {
            this.maxPendingFunc = func;
            return this;
        }
        
        public Builder<Host, Client, Tracker> withMaxAcquiredStrategy(Func1<Integer, Integer> func) {
            this.maxAcquiredFunc = func;
            return this;
        }
        
        public Builder<Host, Client, Tracker> withConnector(HostClientConnector<Host, Client> connector) {
            this.connector = connector;
            return this;
        }
        
        public Builder<Host, Client, Tracker> withHostSource(Observable<HostEvent<Host>> hostSource) {
            this.hostSource = hostSource;
            return this;
        }
        
        public Builder<Host, Client, Tracker> withClientTrackerFactory(ClientTrackerFactory<Host, Tracker> failureDetectorFactory) {
            this.clientTrackerFactory = failureDetectorFactory;
            return this;
        }
        
        public Builder<Host, Client, Tracker> withWeightingStrategy(WeightingStrategy<Host, Client, Tracker> algorithm) {
            this.weightingStrategy = algorithm;
            return this;
        }
        
        public DefaultClientSelector<Host, Client, Tracker> build() {
            assert connector != null;
            assert clientTrackerFactory != null;
            assert hostSource != null;
            
            return new DefaultClientSelector<Host, Client, Tracker>(this);
        }
    }
    
    public static <Host, Client, Tracker extends Action1<ClientEvent>> Builder<Host, Client, Tracker> builder() {
        return new Builder<Host, Client, Tracker>();
    }
    
    private final Func1<HostContext<Host, Client, Tracker>, Boolean> NOT_REMOVED = new Func1<HostContext<Host, Client, Tracker>, Boolean>() {
        @Override
        public Boolean call(HostContext<Host, Client, Tracker> context) {
            return !context.isRemoved();
        }
    };
    
    /**
     * Externally provided factory for creating a Client from a Host
     */
    private final HostClientConnector<Host, Client> connector;
    
    /**
     * Function that determines the maximum allowed pending Client creations based
     * on the total number of known hosts
     */
    private final Func1<Integer, Integer> maxPendingFunc;
    
    /**
     * Function that determines the maximum allowed Clients based on the 
     * total number of known hosts
     */
    private final Func1<Integer, Integer> maxAcquiredFunc;

    /**
     * Factory used to create Tracker objects that gather statistics on clients
     * and implement failure detection logic
     */
    private final ClientTrackerFactory<Host, Tracker> clientTrackerFactory;

    /**
     * Map of ALL existing hosts, connected or not
     */
    private Map<Host, HostContext<Host, Client, Tracker>> hosts = new HashMap<Host, HostContext<Host, Client, Tracker>>();
    
    /**
     * Queue of available hosts.  This list does not include any of the active clients
     * 
     * TODO: Change this to a semaphore and array list from which a random client can be acquired
     */
    private BlockingQueue<HostContext<Host, Client, Tracker>> availableHosts = new LinkedBlockingQueue<HostContext<Host, Client, Tracker>>();
    
    /**
     * Map of ALL currently acquired clients
     */
    private Map<Client, HostContext<Host, Client, Tracker>>   acquiredClients = new IdentityHashMap<Client, HostContext<Host, Client, Tracker>>();
    
    private CopyOnWriteArrayList<HostContext<Host, Client, Tracker>> activeClients = new CopyOnWriteArrayList<HostContext<Host, Client, Tracker>>();
    
    /**
     * Number of pending Client creations.  Used to limit concurrent connects
     */
    private final AtomicInteger pendingClientCreates = new AtomicInteger();
    
    private final Observable<HostEvent<Host>> hostSource;
    
    private final CompositeSubscription cs = new CompositeSubscription();
    
    private AtomicLong activeRequests = new AtomicLong();
    
    private volatile int activeClientCount = 0;
    
    private final WeightingStrategy<Host, Client, Tracker> weightingStrategy;

    private DefaultClientSelector(Builder<Host, Client, Tracker> builder) {
        this.connector = builder.connector;
        this.clientTrackerFactory = builder.clientTrackerFactory;
        this.hostSource = builder.hostSource;
        this.maxAcquiredFunc = builder.maxAcquiredFunc;
        this.maxPendingFunc = builder.maxPendingFunc;
        this.weightingStrategy = builder.weightingStrategy;
    }

    public void initialize() {
        cs.add(hostSource.subscribe(new Action1<HostEvent<Host>>() {
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
        }));
    }
    
    public void shutdown() {
        cs.unsubscribe();
    }
    
    private synchronized void addHost(final Host host) {
        LOG.info("Adding host : " + host);
        
        HostContext<Host, Client, Tracker> context = new HostContext<Host, Client, Tracker>(host);
        context.setClientTracker(clientTrackerFactory.call(context.getHost(), new Action0() {
            @Override
            public void call() {
                LOG.info("Quarantine host");
                quarantineHost(host);
            }
        }));
        
        hosts.put(host, context);
        availableHosts.add(context);
    }
    
    private synchronized void removeHost(Host host) {
        LOG.info("Removing host : " + host);
        
        HostContext<Host, Client, Tracker> context = hosts.remove(host);
        if (context != null) {
            context.remove();
            if (context.getClient() != null) {
                activeClients.remove(context);
                acquiredClients.remove(context.getClient());
                activeClientCount--;
            }
        }
    }
    
    private synchronized void quarantineHost(Host host) {
        HostContext<Host, Client, Tracker> context = hosts.remove(host);
        if (context != null) {
            activeClients.remove(context);
        }
    }

    private boolean activateClient(final HostContext<Host, Client, Tracker> context, Client client) {
        LOG.info("Activate client host : " + client);
        
        context.setClient(client);

        if (context.isRemoved()) {
            return false;
        }
        else {
            activeClients.add(context);
            acquiredClients.put(client, context);
            activeClientCount++;
            return true;
        }
    }
    
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
    
    private Observable<HostContext<Host, Client, Tracker>> nextAvailable() {
        return Observable.create(new OnSubscribe<HostContext<Host, Client, Tracker>>() {
            @Override
            public void call(Subscriber<? super HostContext<Host, Client, Tracker>> t1) {
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
            .concatMap(new Func1<HostContext<Host, Client, Tracker>, Observable<Client>>() {
                @Override
                public Observable<Client> call(final HostContext<Host, Client, Tracker> context) {
                    return connector
                        .call(context.getHost(), new Action1<ClientEvent>() {
                            @Override
                            public void call(ClientEvent event) {
                                LOG.info("Got event: " + event + " " + context.getHost());
                                context.getClientTracker().call(event);
                                
                                switch (event.getType()) {
                                case REMOVED:
                                    removeHost(context.getHost());
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
                        .doOnError(RxUtil.error("Failed to connect to : " + context))
                        .filter(new Func1<Client, Boolean>() {
                            @Override
                            public Boolean call(Client client) {
                                return activateClient(context, client);
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
    public Observable<ClientsAndWeights<Client>> aquire() {
        return Observable.create(new OnSubscribe<ClientsAndWeights<Client>>() {
            @Override
            public void call(final Subscriber<? super ClientsAndWeights<Client>> child) {
                // If no active clients then try to connect one client
                if (activeClients.isEmpty()) {
                    connect()
                        .finallyDo(new Action0() {
                            @Override
                            public void call() {
                                child.onNext(weightingStrategy.call(new ArrayList<HostContext<Host, Client, Tracker>>(activeClients)));
                                child.onCompleted();
                            }
                        })
                        .subscribe();
                }
                // Return the most recent list of clients
                else {
                    child.onNext(weightingStrategy.call(new ArrayList<HostContext<Host, Client, Tracker>>(activeClients)));
                    child.onCompleted();
                }
            }
        });
    }
}
