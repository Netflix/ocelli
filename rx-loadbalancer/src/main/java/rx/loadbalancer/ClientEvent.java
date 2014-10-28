package rx.loadbalancer;

import java.util.concurrent.TimeUnit;

public class ClientEvent {
    public enum Type {
        // The Host was removed
        REMOVED,
        
        // The Client is attempting to open a connection to the host
        CONNECT_START,
        
        // THe Client opened a connection to the host
        CONNECT_SUCCESS,
        
        // The client failed to open a connection 
        CONNECT_FAILURE,
        
        // The client is starting a request on one of it's connections
        REQUEST_START,
        
        // The client request returned successfully
        REQUEST_SUCCESS,
        
        // The client request failed. This also means that the client connection
        // failed.  This should not be used to count non-retryable errors such
        // as bad requests (such as HTTP 404).
        REQUEST_FAILURE,
        
        // This is a specialization of REQUEST_FAILURE in that it 
        // should be treated differently for the purposes of identifying excessive
        // load situations rather than excessive failure situations
        REQUEST_THROTTLED,
    }
    
    private final long duration;
    private final TimeUnit units;
    private final Type type;
    private final Throwable e;

    public static ClientEvent connectStart() {
        return new ClientEvent(Type.CONNECT_START);
    }
    
    public static ClientEvent connectFailure(long elapsed, TimeUnit units, Throwable error) {
        return new ClientEvent(Type.CONNECT_FAILURE, elapsed, units, error);
    }
    
    public static ClientEvent connectSuccess(long elapsed, TimeUnit units) {
        return new ClientEvent(Type.CONNECT_SUCCESS, elapsed, units);
    }
    
    public static ClientEvent requestStart() {
        return new ClientEvent(Type.REQUEST_START);
    }
    
    public static ClientEvent requestFailure(long elapsed, TimeUnit units, Throwable error) {
        return new ClientEvent(Type.REQUEST_FAILURE, elapsed, units, error);
    }
    
    public static ClientEvent requestSuccess(long elapsed, TimeUnit units) {
        return new ClientEvent(Type.REQUEST_SUCCESS, elapsed, units);
    }
    
    public ClientEvent(Type type) {
        this(type, 0, null, null);
    }
    
    public ClientEvent(Type type, long duration, TimeUnit units) {
        this(type, duration, units, null);
    }
    
    public ClientEvent(Type type, long duration, TimeUnit units, Throwable error) {
        this.type = type;
        this.duration = duration;
        this.units = units;
        this.e = error;
    }
    
    public long getDuration(TimeUnit units) {
        if (units == null)
            return 0;
        return units.convert(duration, this.units);
    }
    
    public Type getType() {
        return type;
    }
    
    public Throwable getError() {
        return e;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ClientEvent[")
          .append("type=").append(type);
        
        sb.append("]");
        return sb.toString();
    }
}
