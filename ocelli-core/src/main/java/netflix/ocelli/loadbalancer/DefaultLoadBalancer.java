package netflix.ocelli.loadbalancer;

import netflix.ocelli.ClientConnector;
import netflix.ocelli.FailureDetectorFactory;
import netflix.ocelli.ManagedLoadBalancer;
import netflix.ocelli.MembershipEvent;
import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.WeightingStrategy;
import netflix.ocelli.selectors.ClientsAndWeights;
import netflix.ocelli.util.RandomBlockingQueue;
import netflix.ocelli.util.RxUtil;
import netflix.ocelli.util.StateMachine;
import netflix.ocelli.util.StateMachine.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.SerialSubscription;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The ClientSelector keeps track of all existing hosts and returns a single host for each
 * call to acquire().
 * 
 * @author elandau
 *
 * @param <C>
 *
 */
public class DefaultLoadBalancer<C> implements ManagedLoadBalancer<C> {
    
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancer.class);
    
    private final Observable<MembershipEvent<C>> hostSource;
    private final WeightingStrategy<C> weightingStrategy;
    private final FailureDetectorFactory<C> failureDetector;
    private final ClientConnector<C> clientConnector;
    private final Func1<Integer, Integer> connectedHostCountStrategy;
    private final Func1<Integer, Long> quaratineDelayStrategy;
    private final Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy;
    
    private final String name;

    /**
     * Composite subscription to keep track of all Subscriptions to be unsubscribed at
     * shutdown
     */
    private final CompositeSubscription cs = new CompositeSubscription();
    
    /**
     * Map of ALL existing hosts, connected or not
     */
    private final ConcurrentMap<C, Holder> clients = new ConcurrentHashMap<C, Holder>();
    
    /**
     * Queue of idle hosts that are not initialized to receive traffic
     */
    private final RandomBlockingQueue<Holder> idleClients = new RandomBlockingQueue<Holder>();
    
    /**
     * Map of ALL currently acquired clients.  This map contains both connected as well as connecting hosts.
     */
    private final Set<Holder> acquiredClients = new HashSet<Holder>();
    
    /**
     * Array of active and healthy clients that can receive traffic
     */
    private final CopyOnWriteArrayList<C> activeClients = new CopyOnWriteArrayList<C>();
    
    private State<Holder, EventType> IDLE         = State.create("IDLE");
    private State<Holder, EventType> CONNECTING   = State.create("CONNECTING");
    private State<Holder, EventType> CONNECTED    = State.create("CONNECTED");
    private State<Holder, EventType> QUARANTINED  = State.create("QUARANTINED");
    private State<Holder, EventType> REMOVED      = State.create("REMOVED");

    public DefaultLoadBalancer(
            String name, 
            Observable<MembershipEvent<C>> hostSource, 
            ClientConnector<C> clientConnector, 
            FailureDetectorFactory<C> failureDetector, 
            Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy, 
            Func1<Integer, Long> quaratineDelayStrategy, 
            Func1<Integer, Integer> connectedHostCountStrategy, 
            WeightingStrategy<C> weightingStrategy) {
        this.weightingStrategy          = weightingStrategy;
        this.connectedHostCountStrategy = connectedHostCountStrategy;
        this.quaratineDelayStrategy     = quaratineDelayStrategy;
        this.selectionStrategy          = selectionStrategy;
        this.name                       = name;
        this.failureDetector            = failureDetector;
        this.clientConnector            = clientConnector;
        this.hostSource                 = hostSource;
        
        initialize();
    }

    private void initialize() {
        
        IDLE
            .onEnter(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(Holder holder) {
                    LOG.info("{} - {} is idle", name, holder.getClient());
                    
                    idleClients.add(holder);
    
                    // Determine if a new host should be created based on the configured strategy
                    int idealCount = connectedHostCountStrategy.call(clients.size());
                    if (idealCount > acquiredClients.size()) {
                        acquireNextIdleHost()  
                            .first()
                            .subscribe(new Action1<Holder>() {
                                @Override
                                public void call(Holder holder) {
                                    holder.sm.call(EventType.CONNECT);
                                }
                            });
                    }
                    return Observable.empty();
                }
            })
            .transition(EventType.CONNECT, CONNECTING)
            .transition(EventType.FAILED, QUARANTINED)
            .transition(EventType.CONNECTED, CONNECTED)
            ;
        
        CONNECTING
            .onEnter(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(final Holder holder) {
                    LOG.info("{} - {} is connecting", name, holder.getClient());
                    acquiredClients.add(holder);
                    holder.connect();
                    return Observable.empty();
                }
            })
            .transition(EventType.CONNECTED, CONNECTED)
            .transition(EventType.FAILED, QUARANTINED)
            .transition(EventType.REMOVE, REMOVED)
            ;
        
        CONNECTED
            .onEnter(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(Holder holder) {
                    LOG.info("{} - {} is connected", name, holder.getClient());
                    activeClients.add(holder.client);
                    return Observable.empty();
                }
            })
            .onExit(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(Holder holder) {
                    activeClients.remove(holder.client);
                    return Observable.empty();
                }
            })
            .ignore(EventType.CONNECTED)
            .ignore(EventType.CONNECT)
            .transition(EventType.FAILED, QUARANTINED)
            .transition(EventType.REMOVE, REMOVED)
            .transition(EventType.STOP, IDLE)
            ;
        
        QUARANTINED
            .onEnter(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(final Holder holder) {
                    LOG.info("{} - {} is quaratined ({})", name, holder.getClient(), holder.quaratineCounter);
                    acquiredClients.remove(holder);
                    
                    return Observable
                            .just(EventType.UNQUARANTINE)
                            .delay(quaratineDelayStrategy.call(holder.getQuaratineCounter()), TimeUnit.MILLISECONDS)
                            .doOnNext(RxUtil.info("Next:")); 
                }
            })
            .ignore(EventType.FAILED)
            .transition(EventType.UNQUARANTINE, IDLE)
            .transition(EventType.REMOVE, REMOVED)
            .transition(EventType.CONNECTED, CONNECTED)
            ;
        
        REMOVED
            .onEnter(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(Holder holder) {
                    LOG.info("{} - {} is removed", name, holder.getClient());
                    activeClients.remove(holder);
                    acquiredClients.add(holder);
                    idleClients.remove(holder);
                    clients.remove(holder.client);
                    cs.remove(holder.cs);
                    return Observable.empty();
                }
        })
        ;
        cs.add(hostSource
            .subscribe(new Action1<MembershipEvent<C>>() {
                @Override
                public void call(MembershipEvent<C> event) {
                    Holder holder = clients.get(event.getClient());
                    if (holder == null) {
                        if (event.getType().equals(EventType.ADD)) {
                            final Holder newHolder = new Holder(event.getClient(), IDLE);
                            if (null == clients.putIfAbsent(event.getClient(), newHolder)) {
                                LOG.trace("{} - {} is added", name, newHolder.getClient());
                                newHolder.initialize();
                            }
                        }
                    }
                    else {
                        holder.sm.call(event.getType());
                    }
                }
            }));
    }
    
    /**
     * Holder the client state within the context of this LoadBalancer
     */
    public class Holder {
        final AtomicInteger quaratineCounter = new AtomicInteger();
        final C client;
        final StateMachine<Holder, EventType> sm;
        final CompositeSubscription cs = new CompositeSubscription();
        final SerialSubscription connectSubscription = new SerialSubscription();
        
        public Holder(C client, State<Holder, EventType> initial) {
            this.client = client;
            this.sm = StateMachine.create(this, initial);
        }
        
        public void initialize() {
            this.cs.add(sm.start().subscribe());
            this.cs.add(connectSubscription);
            this.cs.add(failureDetector.call(client).subscribe(new Action1<Throwable>() {
                @Override
                public void call(Throwable t1) {
                    sm.call(EventType.FAILED);
                    quaratineCounter.incrementAndGet();
                }
            }));
        }
        
        public void connect() {
            connectSubscription.set(clientConnector.call(client).subscribe(
                new Action1<C>() {
                    @Override
                    public void call(C client) {
                        sm.call(EventType.CONNECTED);
                        quaratineCounter.set(0);
                    }
                },
                new Action1<Throwable>() {
                    @Override
                    public void call(Throwable t1) {
                        sm.call(EventType.FAILED);
                        quaratineCounter.incrementAndGet();
                    }
                }));
        }
        
        public int getQuaratineCounter() {
            return quaratineCounter.get();
        }

        public void shutdown() {
            cs.unsubscribe();
        }
        
        public String toString() {
            return "Holder[" + name + "-" + client + "]";
        }

        public C getClient() {
            return client;
        }
    }
    
    public void shutdown() {
        cs.unsubscribe();
    }
    
    /**
     * @return Return the next idle host or empty() if none available
     */
    private Observable<Holder> acquireNextIdleHost() {
        return Observable.create(new OnSubscribe<Holder>() {
            @Override
            public void call(Subscriber<? super Holder> s) {
                try {
                    Holder holder = idleClients.poll();
                    if (holder != null)
                        s.onNext(holder);
                    s.onCompleted();
                }
                catch (Exception e) {
                    s.onError(e);
                }
            }
        });
    }
    
    /**
     * Acquire the most recent list of hosts
     */
    @Override
    public Observable<C> choose() {
        if (activeClients.isEmpty()) {
            return Observable.error(new IllegalArgumentException("No servers available in the load balancer: " + name));
        }
        return selectionStrategy.call(
                weightingStrategy.call(new ArrayList<C>(activeClients)));
    }

    @Override
    public Observable<C> listAllClients() {
        return Observable.from(new HashSet<C>(clients.keySet()));
    }
    
    @Override
    public Observable<C> listActiveClients() {
        return Observable.from(activeClients);
    }
    
    public String getName() {
        return name;
    }
    
    public String toString() {
        return "DefaultLoadBalancer[" + name + "]";
    }
}
