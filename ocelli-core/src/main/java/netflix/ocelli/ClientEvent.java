package netflix.ocelli;

import netflix.ocelli.metrics.MetricsEvent;

public enum ClientEvent implements MetricsEvent<ClientEvent> {
    // The Client is attempting to open a connection to the host
    CONNECT_START(false, false),
    
    // THe Client opened a connection to the host
    CONNECT_SUCCESS(true, false),
    
    // The client failed to open a connection 
    CONNECT_FAILURE(true, true),
    
    // The client is starting a request on one of it's connections
    REQUEST_START(false, false),
    
    // The client request returned successfully
    REQUEST_SUCCESS(true, false),
    
    // The client request failed. This also means that the client connection
    // failed.  This should not be used to count non-retryable errors such
    // as bad requests (such as HTTP 404).
    REQUEST_FAILURE(true, true),
    
    // This is a specialization of REQUEST_FAILURE in that it 
    // should be treated differently for the purposes of identifying excessive
    // load situations rather than excessive failure situations
    REQUEST_THROTTLED(false, false);

    private final boolean isTimed;
    private final boolean isError;

    ClientEvent(boolean isTimed, boolean isError) {
        this.isError = isError;
        this.isTimed = isTimed;
    }
    
    @Override
    public boolean isTimed() {
        return isTimed;
    }

    @Override
    public boolean isError() {
        return isError;
    }
}
