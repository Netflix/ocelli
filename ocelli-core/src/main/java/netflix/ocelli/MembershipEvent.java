package netflix.ocelli;

import rx.functions.Func1;

public class MembershipEvent<Client> {
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
    
    private final Client host;
    private final EventType type;
    
    public static <Client> MembershipEvent<Client> create(Client host, EventType type) {
        return new MembershipEvent<Client>(type, host);
    }
    
    public static <Client> Func1<Client, MembershipEvent<Client>> toEvent(final EventType type) {
        return new Func1<Client, MembershipEvent<Client>>() {
            @Override
            public MembershipEvent<Client> call(Client host) {
                return MembershipEvent.create(host, type);
            }
        };
    }
    
    public static <Client> Func1<MembershipEvent<Client>, MembershipEvent.EventType> byAction() {
        return new Func1<MembershipEvent<Client>, MembershipEvent.EventType>() {
            @Override
            public EventType call(MembershipEvent<Client> t1) {
                return t1.getType();
            }
        };
    }
    
    public MembershipEvent(EventType type, Client host) {
        this.type = type;
        this.host = host;
    }
    
    public EventType getType() {
        return this.type;
    }
    
    public Client getClient() {
        return this.host;
    }
   
    @Override
    public String toString() {
        return "HostEvent [" + type + " " + host + "]";
    }
}
