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

import rx.Notification;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.loadbalancer.ClientEvent;
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.HostEvent.EventType;
import rx.loadbalancer.LoadBalancer;
import rx.loadbalancer.ManagedClient;
import rx.loadbalancer.WeightingStrategy;
import rx.loadbalancer.selectors.ClientsAndWeights;
import rx.loadbalancer.util.RandomBlockingQueue;
import rx.loadbalancer.util.RxUtil;
import rx.loadbalancer.util.StateMachine;
import rx.loadbalancer.util.StateMachine.State;
import rx.subjects.PublishSubject;
import rx.subscriptions.CompositeSubscription;

public abstract class AbstractLoadBalancer<H, C, M extends Action1<ClientEvent>> implements LoadBalancer<H, C, M> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancer.class);
    
    /**
     * Holder the client state within the context of this LoadBalancer
     */
    public class Holder {
        int quaratineCounter = 0;
        final ManagedClient<H, C, M> client;
        final StateMachine<Holder, EventType> sm;
        final CompositeSubscription cs = new CompositeSubscription();
        
        public Holder(ManagedClient<H, C, M> client, State<Holder, EventType> initial) {
            this.client = client;
            this.sm = StateMachine.create(this, initial);
        }
        
        public int incQuaratineCounter() {
            return ++this.quaratineCounter;
        }
        
        public void resetQuaratineCounter() {
            this.quaratineCounter = 0;
        }
        
        public int getQuaratineCounter() {
            return this.quaratineCounter;
        }

        public Subscription start() {
            cs.add(sm.start().subscribe());
            cs.add(client.notifications().subscribe(new Action1<Notification<C>>() {
                @Override
                public void call(Notification<C> n) {
                    switch (n.getKind()) {
                    case OnNext:
                        LOG.info("{} : onNext {}", name, n.getValue());
                        sm.call(EventType.CONNECTED);
                        break;
                    case OnError:
                        sm.call(EventType.FAILED);
                        break;
                    case OnCompleted:
                        break;
                    }
                }
            }));
            return cs;
        }
        
        public String toString() {
            return "Holder[" + name + "-" + client.getHost() + "]";
        }
    }
    
    /**
     * Composite subscription to keep track of all Subscriptions to be unsubscribed at
     * shutdown
     */
    protected final CompositeSubscription cs = new CompositeSubscription();
    
    /**
     * Strategy use to calculate weights for active clients
     */
    protected final WeightingStrategy<H, C, M> weightingStrategy;

    /**
     * Strategy used to determine how many hosts should be connected.
     * This strategy is invoked whenever a host is added or removed from the pool
     */
    protected Func1<Integer, Integer> connectedHostCountStrategy;

    /**
     * Strategy used to determine the delay time in msec based on the quarantine 
     * count.  The count is incremented by one for each failed connect.
     */
    protected Func1<Integer, Long> quaratineDelayStrategy;

    /**
     * Number of total pending requests
     */
    protected AtomicLong activeRequests = new AtomicLong();
    
    /**
     * Map of ALL existing hosts, connected or not
     */
    protected ConcurrentMap<H, Holder> hosts = new ConcurrentHashMap<H, Holder>();
    
    /**
     * Queue of idle hosts that are not initialized to receive traffic
     */
    protected RandomBlockingQueue<Holder> idleHosts = new RandomBlockingQueue<Holder>();
    
    /**
     * Map of ALL currently acquired clients.  This map contains both connected as well as connecting hosts.
     */
    protected Set<Holder> acquiredClients = new HashSet<Holder>();
    
    /**
     * Array of active and healthy clients that can receive traffic
     */
    protected CopyOnWriteArrayList<ManagedClient<H, C, M>> activeClients = new CopyOnWriteArrayList<ManagedClient<H, C, M>>();
    
    protected PublishSubject<HostEvent<H>> eventStream = PublishSubject.create();

    protected Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy;

    protected final String name;
    
    protected AbstractLoadBalancer(
            String name, 
            WeightingStrategy<H, C, M> weightingStrategy, 
            Func1<Integer, Integer> connectedHostCountStrategy, 
            Func1<Integer, Long> quaratineDelayStrategy, 
            Func1<ClientsAndWeights<C>, Observable<C>> selectionStrategy) {
        this.weightingStrategy = weightingStrategy;
        this.connectedHostCountStrategy = connectedHostCountStrategy;
        this.quaratineDelayStrategy = quaratineDelayStrategy;
        this.selectionStrategy = selectionStrategy;
        this.name = name;
    }

    protected State<Holder, EventType> IDLE         = State.create("IDLE");
    protected State<Holder, EventType> CONNECTING   = State.create("CONNECTING");
    protected State<Holder, EventType> CONNECTED    = State.create("CONNECTED");
    protected State<Holder, EventType> QUARANTINED  = State.create("QUARANTINED");
    protected State<Holder, EventType> REMOVED      = State.create("REMOVED");
    
    public void initialize() {
        IDLE
            .onEnter(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(Holder holder) {
                    eventStream.onNext(HostEvent.create(holder.client.getHost(), EventType.IDLE));

                    idleHosts.add(holder);

                    // Determine if a new host should be created based on the configured strategy
                    int idealCount = connectedHostCountStrategy.call(hosts.size());
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
                    LOG.info("Client is connecting");
                    holder.client.connect()
                        .doOnError(RxUtil.error("Failed to connect"))
                        .doOnNext(RxUtil.info("Connected to"))
                        .subscribe();
                    eventStream.onNext(HostEvent.create(holder.client.getHost(), EventType.CONNECT));
                    acquiredClients.add(holder);
                    return holder.client.connect().ignoreElements().cast(EventType.class);
//                        .call(context.getHost(), decorateHostContextTracker(context), context.getShutdownSubject())
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
                    LOG.info("Client is now connected");
                    eventStream.onNext(HostEvent.create(holder.client.getHost(), EventType.CONNECTED));
                    activeClients.add(holder.client);
                    return Observable.empty();
                }
            })
            .onExit(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(Holder holder) {
                    activeClients.remove(holder.client);
                    acquiredClients.add(holder);
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
                    eventStream.onNext(HostEvent.create(holder.client.getHost(), EventType.FAILED));
                    acquiredClients.remove(holder);
                    
                    return Observable
                            .just(EventType.UNQUARANTINE)
                            .delay(quaratineDelayStrategy.call(holder.incQuaratineCounter()), TimeUnit.MILLISECONDS)
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
                    eventStream.onNext(HostEvent.create(holder.client.getHost(), EventType.REMOVE));
                    
                    activeClients.remove(holder);
                    acquiredClients.add(holder);
                    idleHosts.remove(holder);
                    hosts.remove(holder.client.getHost());
                    cs.remove(holder.cs);
                    return Observable.empty();
                }
            })
            ;
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
                    Holder holder;
                    
                    do {
                        holder = idleHosts.poll();
                    } while (holder == null);
                    
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
        return selectionStrategy.call(
                    weightingStrategy.call(new ArrayList<ManagedClient<H, C, M>>(activeClients)));
    }

    @Override
    public Observable<C> choose(H host) {
        Holder holder = hosts.get(host);
        if (holder == null) {
            return Observable.empty();
        }
        return holder.client.connect();
    }

    @Override
    public Observable<HostEvent<H>> events() {
        return eventStream;
    }
    
    @Override
    public synchronized Observable<H> listAllHosts() {
        return Observable.from(new HashSet<H>(hosts.keySet()));
    }
    
    @Override
    public synchronized Observable<H> listActiveHosts() {
        return Observable.from(activeClients).map(new Func1<ManagedClient<H, C, M>, H>() {
            @Override
            public H call(ManagedClient<H, C, M> context) {
                return context.getHost();
            }
        });
    }
    
    @Override
    public synchronized Observable<C> listActiveClients() {
        return Observable.from(activeClients).map(new Func1<ManagedClient<H, C, M>, C>() {
            @Override
            public C call(ManagedClient<H, C, M> context) {
                return context.getClient();
            }
        });
    }
    
    @Override
    public ManagedClient<H, C, M> getClient(H host) {
        Holder holder = hosts.get(host);
        if (holder == null)
            return null;
        return holder.client;
    }

    public String getName() {
        return name;
    }
}
