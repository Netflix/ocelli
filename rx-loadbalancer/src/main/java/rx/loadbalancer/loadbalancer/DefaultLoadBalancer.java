package rx.loadbalancer.loadbalancer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.loadbalancer.ClientEvent;
import rx.loadbalancer.LoadBalancer;
import rx.loadbalancer.ClientTrackerFactory;
import rx.loadbalancer.HostClientConnector;
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.HostEvent.EventType;
import rx.loadbalancer.WeightingStrategy;
import rx.loadbalancer.algorithm.EqualWeightStrategy;
import rx.loadbalancer.selectors.ClientsAndWeights;
import rx.loadbalancer.selectors.RoundRobinSelectionStrategy;
import rx.loadbalancer.util.Functions;
import rx.loadbalancer.util.RandomBlockingQueue;
import rx.loadbalancer.util.RxUtil;
import rx.loadbalancer.util.StateMachine.State;
import rx.subjects.PublishSubject;
import rx.subscriptions.CompositeSubscription;

/**
 * The ClientSelector keeps track of all existing hosts and returns a single host for each
 * call to acquire().
 * 
 * @author elandau
 *
 * @param <Host>
 * @param <Client>
 * @param <Tracker>
 * 
 * TODO: Host quarantine 
 */
public class DefaultLoadBalancer<Host, Client, Tracker extends Action1<ClientEvent>> implements LoadBalancer<Host, Client> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancer.class);
    
    /**
     * Da Builder 
     * @author elandau
     *
     * @param <Host>
     * @param <Client>
     * @param <Tracker>
     */
    public static class Builder<Host, Client, Tracker extends Action1<ClientEvent>> {
        private HostClientConnector<Host, Client> connector;
        private ClientTrackerFactory<Host, Tracker> clientTrackerFactory;
        private Observable<HostEvent<Host>> hostSource;
        private WeightingStrategy<Host, Client, Tracker> weightingStrategy = new EqualWeightStrategy<Host, Client, Tracker>();
        private Func1<Integer, Integer> connectedHostCountStrategy = Functions.identity();
        private Func1<Integer, Long> quaratineDelayStrategy;
        private Func1<ClientsAndWeights<Client>, Observable<Client>> selectionStrategy = new RoundRobinSelectionStrategy<Client>();
        
        public Builder<Host, Client, Tracker> withQuaratineStrategy(Func1<Integer, Long> quaratineDelayStrategy) {
            this.quaratineDelayStrategy = quaratineDelayStrategy;
            return this;
        }
        
        public Builder<Host, Client, Tracker> withConnectedHostCountStrategy(Func1<Integer, Integer> connectedHostCountStrategy) {
            this.connectedHostCountStrategy = connectedHostCountStrategy;
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
        
        public Builder<Host, Client, Tracker> withSelectionStrategy(Func1<ClientsAndWeights<Client>, Observable<Client>> selectionStrategy) {
            this.selectionStrategy = selectionStrategy;
            return this;
        }
        
        public DefaultLoadBalancer<Host, Client, Tracker> build() {
            assert connector != null;
            assert clientTrackerFactory != null;
            assert hostSource != null;
            
            return new DefaultLoadBalancer<Host, Client, Tracker>(this);
        }
    }
    
    public static <Host, Client, Tracker extends Action1<ClientEvent>> Builder<Host, Client, Tracker> builder() {
        return new Builder<Host, Client, Tracker>();
    }
    
    /**
     * Externally provided factory for creating a Client from a Host
     */
    private final HostClientConnector<Host, Client> connector;
    
    /**
     * Factory used to create Tracker objects that gather statistics on clients
     * and implement failure detection logic
     */
    private final ClientTrackerFactory<Host, Tracker> clientTrackerFactory;

    /**
     * Source for host membership events
     */
    private final Observable<HostEvent<Host>> hostSource;
    
    /**
     * Composite subscription to keep track of all Subscriptions to be unsubscribed at
     * shutdown
     */
    private final CompositeSubscription cs = new CompositeSubscription();
    
    /**
     * Strategy use to calculate weights for active clients
     */
    private final WeightingStrategy<Host, Client, Tracker> weightingStrategy;

    /**
     * Strategy used to determine how many hosts should be connected.
     * This strategy is invoked whenever a host is added or removed from the pool
     */
    private Func1<Integer, Integer> connectedHostCountStrategy;

    /**
     * Strategy used to determine the delay time in msec based on the quarantine 
     * count.  The count is incremented by one for each failed connect.
     */
    private Func1<Integer, Long> quaratineDelayStrategy;

    /**
     * Number of total pending requests
     */
    private AtomicLong activeRequests = new AtomicLong();
    
    /**
     * Map of ALL existing hosts, connected or not
     */
    private ConcurrentMap<Host, HostContext<Host, Client, Tracker>> hosts = new ConcurrentHashMap<Host, HostContext<Host, Client, Tracker>>();
    
    /**
     * Queue of idle hosts that are not initialized to receive traffic
     */
    private RandomBlockingQueue<HostContext<Host, Client, Tracker>> idleHosts = new RandomBlockingQueue<HostContext<Host, Client, Tracker>>();
    
    /**
     * Map of ALL currently acquired clients.  This map contains both connected as well as connecting hosts.
     */
    private Set<HostContext<Host, Client, Tracker>> acquiredClients = new HashSet<HostContext<Host, Client, Tracker>>();
    
    /**
     * Array of active and healthy clients that can receive traffic
     */
    private CopyOnWriteArrayList<HostContext<Host, Client, Tracker>> activeClients = new CopyOnWriteArrayList<HostContext<Host, Client, Tracker>>();
    
    private PublishSubject<HostEvent<Host>> eventStream = PublishSubject.create();

    private Func1<ClientsAndWeights<Client>, Observable<Client>> selectionStrategy;
    
    private DefaultLoadBalancer(Builder<Host, Client, Tracker> builder) {
        this.connector = builder.connector;
        this.clientTrackerFactory = builder.clientTrackerFactory;
        this.hostSource = builder.hostSource;
        this.weightingStrategy = builder.weightingStrategy;
        this.connectedHostCountStrategy = builder.connectedHostCountStrategy;
        this.quaratineDelayStrategy = builder.quaratineDelayStrategy;
        this.selectionStrategy = builder.selectionStrategy;
    }

    private State<HostContext<Host, Client, Tracker>, EventType> IDLE         = State.create("IDLE");
    private State<HostContext<Host, Client, Tracker>, EventType> CONNECTING   = State.create("CONNECTING");
    private State<HostContext<Host, Client, Tracker>, EventType> CONNECTED    = State.create("CONNECTED");
    private State<HostContext<Host, Client, Tracker>, EventType> QUARANTINED  = State.create("QUARANTINED");
    private State<HostContext<Host, Client, Tracker>, EventType> REMOVED      = State.create("REMOVED");
    
    public void initialize() {
        IDLE
            .onEnter(new Func1<HostContext<Host, Client, Tracker>, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(HostContext<Host, Client, Tracker> context) {
                    eventStream.onNext(HostEvent.create(context.getHost(), EventType.IDLE));

                    idleHosts.add(context);

                    // Determine if a new host should be created based on the configured strategy
                    int idealCount = connectedHostCountStrategy.call(hosts.size());
                    if (idealCount > acquiredClients.size()) {
                        acquireNextIdleHost()  
                            .first()
                            .subscribe(fireEvent(EventType.CONNECT));

                    }
                    return Observable.empty();
                }
            })
            .transition(EventType.CONNECT, CONNECTING)
            ;
        
        CONNECTING
            .onEnter(new Func1<HostContext<Host, Client, Tracker>, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(final HostContext<Host, Client, Tracker> context) {
                    eventStream.onNext(HostEvent.create(context.getHost(), EventType.CONNECT));
                    acquiredClients.add(context);
                    return connector
                        .call(context.getHost(), decorateHostContextTracker(context), context.getShutdownSubject())
                        .map(new Func1<Client, EventType>() {
                            @Override
                            public EventType call(Client client) {
                                context.setClient(client);
                                return EventType.CONNECTED;
                            }
                        })
                        .onErrorResumeNext(Observable.<EventType>empty());
                }
            })
            .transition(EventType.CONNECTED, CONNECTED)
            .transition(EventType.FAILED, QUARANTINED)
            .transition(EventType.REMOVE, REMOVED)
            ;
        
        CONNECTED
            .onEnter(new Func1<HostContext<Host, Client, Tracker>, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(HostContext<Host, Client, Tracker> context) {
                    eventStream.onNext(HostEvent.create(context.getHost(), EventType.CONNECTED));
                    activeClients.add(context);
                    return Observable.empty();
                }
            })
            .onExit(new Func1<HostContext<Host, Client, Tracker>, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(HostContext<Host, Client, Tracker> context) {
                    activeClients.remove(context);
                    acquiredClients.add(context);
                    return Observable.empty();
                }
            })
            .transition(EventType.FAILED, QUARANTINED)
            .transition(EventType.REMOVE, REMOVED)
            .transition(EventType.STOP, IDLE)
            ;
        
        QUARANTINED
            .onEnter(new Func1<HostContext<Host, Client, Tracker>, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(final HostContext<Host, Client, Tracker> context) {
                    eventStream.onNext(HostEvent.create(context.getHost(), EventType.FAILED));
                    acquiredClients.remove(context);
                    activeClients.remove(context);
                    
                    return Observable
                            .just(EventType.UNQUARANTINE)
                            .delay(quaratineDelayStrategy.call(context.incQuaratineCounter()), TimeUnit.MILLISECONDS)
                            .doOnNext(RxUtil.info("Next:")); 
                }
            })
            .transition(EventType.UNQUARANTINE, IDLE)
            .transition(EventType.REMOVE, REMOVED)
            ;
        
        REMOVED
            .onEnter(new Func1<HostContext<Host, Client, Tracker>, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(HostContext<Host, Client, Tracker> context) {
                    eventStream.onNext(HostEvent.create(context.getHost(), EventType.REMOVE));
                    context.setRemoved();
                    
                    activeClients.remove(context);
                    acquiredClients.add(context);
                    idleHosts.remove(context);
                    hosts.remove(context);
                    return Observable.empty();
                }
            })
            ;
        
        cs.add(hostSource
            .subscribe(new Action1<HostEvent<Host>>() {
            @Override
            public void call(HostEvent<Host> action) {
                HostContext<Host, Client, Tracker> context = hosts.get(action.getHost());
                if (context == null) {
                    if (action.getAction().equals(EventType.ADD)) {
                        context = new HostContext<Host, Client, Tracker>(action.getHost(), IDLE);
                        if (null == hosts.putIfAbsent(action.getHost(), context)) {
                            context.setClientTracker(clientTrackerFactory.call(context.getHost(), fireEvent(context, EventType.FAILED)));
                            eventStream.onNext(action);
                            cs.add(context.connect());
                        }
                    }
                }
                else {
                    context.call(action.getAction());
                }
            }
        }));
    }
    
    private Action0 fireEvent(final HostContext<Host, Client, Tracker> context, final EventType event) {
        return new Action0() {
            @Override
            public void call() {
                context.call(event);
            }
        };
    }
    
    private Action1<HostContext<Host, Client, Tracker>> fireEvent(final EventType event) {
        return new Action1<HostContext<Host, Client, Tracker>>() {
            @Override
            public void call(HostContext<Host, Client, Tracker> context) {
                context.call(event);
            }
        };
    }
    
    public void shutdown() {
        cs.unsubscribe();
    }
    
    /**
     * @return Return the next idle host or empty() if none available
     */
    private Observable<HostContext<Host, Client, Tracker>> acquireNextIdleHost() {
        return Observable.create(new OnSubscribe<HostContext<Host, Client, Tracker>>() {
            @Override
            public void call(Subscriber<? super HostContext<Host, Client, Tracker>> s) {
                try {
                    HostContext<Host, Client, Tracker> host;
                    
                    do {
                        host = idleHosts.poll();
                    } while (host == null || host.isRemoved());
                    
                    if (host != null)
                        s.onNext(host);
                    s.onCompleted();
                }
                catch (Exception e) {
                    s.onError(e);
                }
            }
        });
    }
    
    /**
     * Decorate the tracker for a specific client so we can keep track of active requests
     * 
     * @param context
     * @return
     */
    private Action1<ClientEvent> decorateHostContextTracker(final HostContext<Host, Client, Tracker> context) {
        return new Action1<ClientEvent>() {
            @Override
            public void call(ClientEvent event) {
//                LOG.info("Got event: " + event + " " + context.getHost());
                context.getClientTracker().call(event);
                
                switch (event.getType()) {
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
        };
    }
    
    /**
     * Acquire the most recent list of hosts
     */
    @Override
    public Observable<Client> select() {
        return Observable.create(new OnSubscribe<Client>() {
            @Override
            public void call(final Subscriber<? super Client> child) {
                selectionStrategy.call(
                    weightingStrategy.call(new ArrayList<HostContext<Host, Client, Tracker>>(activeClients)))
                    .subscribe(child);
            }
        });
    }

    @Override
    public Observable<HostEvent<Host>> events() {
        return eventStream;
    }
    
    public synchronized Observable<Host> listAllHosts() {
        return Observable.from(new HashSet<Host>(hosts.keySet()));
    }
    
    public synchronized Observable<Client> listActiveClients() {
        return Observable.from(activeClients).map(new Func1<HostContext<Host, Client, Tracker>, Client>() {
            @Override
            public Client call(HostContext<Host, Client, Tracker> context) {
                return context.getClient();
            }
        });
    }
}
