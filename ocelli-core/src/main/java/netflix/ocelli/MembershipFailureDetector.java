package netflix.ocelli;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netflix.ocelli.MembershipEvent.EventType;
import netflix.ocelli.functions.Connectors;
import netflix.ocelli.functions.Delays;
import netflix.ocelli.functions.Failures;
import netflix.ocelli.util.RxUtil;
import netflix.ocelli.util.StateMachine;
import netflix.ocelli.util.StateMachine.State;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.SerialSubscription;

/**
 * Operator that manages failure detection for clients and forwards either an add event or remove
 * event based on health of clients.  
 * 
 * @author elandau
 *
 * @param <C>
 */
public class MembershipFailureDetector<C> implements Operator<MembershipEvent<C>, MembershipEvent<C>> {

    private static final Logger LOG = LoggerFactory.getLogger(MembershipFailureDetector.class);
    
    private final FailureDetectorFactory<C> failureDetector;
    private final ClientConnector<C>        clientConnector;
    private final Func1<Integer, Long>      quaratineDelayStrategy;
    private final String                    name;

    public static class Builder<C> {
        private String                      name = "<unnamed>";
        private Func1<Integer, Long>        quaratineDelayStrategy = Delays.fixed(10, TimeUnit.SECONDS);
        private FailureDetectorFactory<C>   failureDetector = Failures.never();
        private ClientConnector<C>          clientConnector = Connectors.immediate();

        /**
         * Arbitrary name assigned to the connection pool, mostly for debugging purposes
         * @param name
         */
        public Builder<C> withName(String name) {
            this.name = name;
            return this;
        }
        
        /**
         * Strategy used to determine the delay time in msec based on the quarantine 
         * count.  The count is incremented by one for each failure detections and reset
         * once the host is back to normal.
         */
        public Builder<C> withQuarantineStrategy(Func1<Integer, Long> quaratineDelayStrategy) {
            this.quaratineDelayStrategy = quaratineDelayStrategy;
            return this;
        }
        
        /**
         * The failure detector returns an Observable that will emit a Throwable for each 
         * failure of the client.  The load balancer will quaratine the client in response.
         * @param failureDetector
         */
        public Builder<C> withFailureDetector(FailureDetectorFactory<C> failureDetector) {
            this.failureDetector = failureDetector;
            return this;
        }
        
        /**
         * The connector can be used to prime a client prior to activating it in the connection
         * pool.  
         * @param clientConnector
         */
        public Builder<C> withClientConnector(ClientConnector<C> clientConnector) {
            this.clientConnector = clientConnector;
            return this;
        }
        
        public MembershipFailureDetector<C> build() {
            return new MembershipFailureDetector<C>(
                    name, 
                    clientConnector, 
                    failureDetector, 
                    quaratineDelayStrategy);
        }
    }
        
    public static <C> Builder<C> builder() {
        return new Builder<C>();
    }
    
    public MembershipFailureDetector(
            String                     name, 
            ClientConnector<C>         clientConnector, 
            FailureDetectorFactory<C>  failureDetector, 
            Func1<Integer, Long>       quaratineDelayStrategy) {
        this.quaratineDelayStrategy     = quaratineDelayStrategy;
        this.name                       = name;
        this.failureDetector            = failureDetector;
        this.clientConnector            = clientConnector;
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
    
    @Override
    public Subscriber<? super MembershipEvent<C>> call(final Subscriber<? super MembershipEvent<C>> s) {
        /**
         * Composite subscription to keep track of all Subscriptions to be un-subscribed at
         * shutdown
         */
        final CompositeSubscription cs = new CompositeSubscription();
        
        /**
         * Map of ALL existing hosts, connected or not
         */
        final ConcurrentMap<C, Holder> clients = new ConcurrentHashMap<C, Holder>();
        
        /**
         * Map of ALL currently acquired clients.  This map contains both connected as well as connecting hosts.
         */
        final Set<Holder> acquiredClients = new HashSet<Holder>();
        
        final State<Holder, EventType> CONNECTING   = State.create("CONNECTING");
        final State<Holder, EventType> CONNECTED    = State.create("CONNECTED");
        final State<Holder, EventType> QUARANTINED  = State.create("QUARANTINED");
        final State<Holder, EventType> REMOVED      = State.create("REMOVED");
            
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
                    s.onNext(MembershipEvent.create(holder.client, EventType.ADD));
                    return Observable.empty();
                }
            })
            .onExit(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(Holder holder) {
                    s.onNext(MembershipEvent.create(holder.client, EventType.REMOVE));
                    return Observable.empty();
                }
            })
            .ignore(EventType.CONNECTED)
            .ignore(EventType.CONNECT)
            .transition(EventType.FAILED, QUARANTINED)
            .transition(EventType.REMOVE, REMOVED)
            ;
        
        QUARANTINED
            .onEnter(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(final Holder holder) {
                    LOG.info("{} - {} is quarantined ({})", name, holder.getClient(), holder.quaratineCounter);
                    acquiredClients.remove(holder);
                    
                    return Observable
                            .just(EventType.UNQUARANTINE)
                            .delay(quaratineDelayStrategy.call(holder.getQuaratineCounter()), TimeUnit.MILLISECONDS)
                            .doOnNext(RxUtil.info("Next:")); 
                }
            })
            .ignore(EventType.FAILED)
            .transition(EventType.UNQUARANTINE, CONNECTING)
            .transition(EventType.REMOVE, REMOVED)
            .transition(EventType.CONNECTED, CONNECTED)
            ;
        
        REMOVED
            .onEnter(new Func1<Holder, Observable<EventType>>() {
                @Override
                public Observable<EventType> call(Holder holder) {
                    LOG.info("{} - {} is removed", name, holder.getClient());
                    acquiredClients.remove(holder);
                    clients.remove(holder.client);
                    cs.remove(holder.cs);
                    s.onNext(MembershipEvent.create(holder.client, EventType.REMOVE));
                    return Observable.empty();
                }
        })
        ;
        
        return new Subscriber<MembershipEvent<C>>() {
            @Override
            public void onCompleted() {
                s.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                s.onError(e);
            }

            @Override
            public void onNext(MembershipEvent<C> event) {
                Holder holder = clients.get(event.getClient());
                if (holder == null) {
                    if (event.getType().equals(EventType.ADD)) {
                        final Holder newHolder = new Holder(event.getClient(), CONNECTING);
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
        };
    }
    
    public String getName() {
        return name;
    }
    
    public String toString() {
        return "MembershipFailureDetector[" + name + "]";
    }
}
