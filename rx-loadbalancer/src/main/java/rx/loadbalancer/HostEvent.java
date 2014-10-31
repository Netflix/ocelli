package rx.loadbalancer;

import rx.functions.Func1;

public class HostEvent<Host> {
    public enum EventType {
        // A new host was added
        ADD,
        
        // A host was removed
        REMOVE,
        
        // The host is connecting
        CONNECT,
        
        // A host connected successfully
        CONNECTED,
        
        // A host failed
        FAILED,

        // The host may be removed from quarantine
        UNQUARANTINE,
        
        // Stop sending traffic to a host
        STOP, 
        
        // The host is now idle either after stopping or being un-quarantined
        IDLE
    }
    
    private final Host host;
    private final EventType action;
    
    public static <Host> HostEvent<Host> create(Host host, EventType type) {
        return new HostEvent<Host>(type, host);
    }
    
    public static <Host> HostEvent<Host> added(Host host) {
        return new HostEvent<Host>(EventType.ADD, host);
    }
    
    public static <Host> HostEvent<Host> removed(Host host) {
        return new HostEvent<Host>(EventType.REMOVE, host);
    }
    
    public static <Host> HostEvent<Host> failed(Host host) {
        return new HostEvent<Host>(EventType.FAILED, host);
    }
    
    public static <Host> HostEvent<Host> stop(Host host) {
        return new HostEvent<Host>(EventType.STOP, host);
    }
    
    public HostEvent(EventType action, Host host) {
        this.action = action;
        this.host = host;
    }
    
    public EventType getAction() {
        return this.action;
    }
    
    public Host getHost() {
        return this.host;
    }

    @Override
    public String toString() {
        return "HostEvent [" + action + " " + host + "]";
    }

    public static <Host> Func1<Host, HostEvent<Host>> toAdd() {
        return new Func1<Host, HostEvent<Host>>() {
            @Override
            public HostEvent<Host> call(Host t1) {
                return HostEvent.added(t1);
            }
        };
    }
    
    public static <Host> Func1<HostEvent<Host>, HostEvent.EventType> byAction() {
        return new Func1<HostEvent<Host>, HostEvent.EventType>() {
            @Override
            public EventType call(HostEvent<Host> t1) {
                return t1.getAction();
            }
        };
    }
}
