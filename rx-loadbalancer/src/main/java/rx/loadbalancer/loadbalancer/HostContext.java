package rx.loadbalancer.loadbalancer;

import rx.Subscription;
import rx.functions.Action1;
import rx.loadbalancer.HostEvent;
import rx.loadbalancer.HostEvent.EventType;
import rx.loadbalancer.util.StateMachine;
import rx.loadbalancer.util.StateMachine.State;
import rx.subjects.PublishSubject;


public class HostContext<Host, Client, Tracker> implements Action1<HostEvent.EventType>{
    private final Host host;
    private Client client;
    private Tracker failureDetector;
    private int quaratineCounter = 0;
    private PublishSubject<Void> shutdown = PublishSubject.create();
    private final StateMachine<HostContext<Host, Client, Tracker>, HostEvent.EventType> sm;
    private boolean removed;
    
    public HostContext(Host host, State<HostContext<Host, Client, Tracker>, HostEvent.EventType> initial) {
        this.host = host;
        this.sm = StateMachine.create(this, initial);
    }
    
    public Host getHost() {
        return host;
    }
    
    public void setClient(Client client) {
        this.client = client;
    }
    
    public Client getClient() {
        return client;
    }
    
    public void setClientTracker(Tracker clientTracker) {
        this.failureDetector = clientTracker;
    }
    
    public Tracker getClientTracker() {
        return failureDetector;
    }
    
    public String toString() {
        return host.toString();
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
    
    public PublishSubject<Void> getShutdownSubject() {
        return shutdown;
    }

    @Override
    public void call(EventType event) {
        sm.call(event);
    }

    public Subscription connect() {
        return sm.connect().subscribe();
    }

    public void setRemoved() {
        removed = true;
    }
    
    public boolean isRemoved() {
        return removed;
    }
}
