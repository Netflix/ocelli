package rx.loadbalancer;

import rx.functions.Func1;

public class HostEvent<Host> {
    public enum Action {
        // A new host was added
        ADD,
        
        // A host was removed
        REMOVE,
    }
    
    private final Host host;
    private final Action action;
    
    public static <Host> HostEvent<Host> added(Host host) {
        return new HostEvent<Host>(Action.ADD, host);
    }
    
    public static <Host> HostEvent<Host> removed(Host host) {
        return new HostEvent<Host>(Action.REMOVE, host);
    }
    
    public static <Host> HostEvent<Host> failed(Host host) {
        return new HostEvent<Host>(Action.REMOVE, host);
    }
    
    public HostEvent(Action action, Host host) {
        this.action = action;
        this.host = host;
    }
    
    public Action getAction() {
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
}
