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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MembershipEvent other = (MembershipEvent) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (type != other.type)
            return false;
        return true;
    }
}
