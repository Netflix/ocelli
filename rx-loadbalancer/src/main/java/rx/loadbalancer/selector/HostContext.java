package rx.loadbalancer.selector;

public class HostContext<Host, Client, Tracker> {
    private final Host host;
    
    private Client client;
    private boolean removed = false;
    private Tracker failureDetector;

    public HostContext(Host host) {
        this.host = host;
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
    
    public void remove() {
        removed = true;
    }
    
    public boolean isRemoved() {
        return removed;
    }

    public String toString() {
        return host.toString();
    }
}
