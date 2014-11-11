package netflix.ocelli.metrics;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import netflix.ocelli.ClientEvent;

public class CompositeClientMetricsListener implements ClientMetricsListener {

    private IdentityHashMap<Class<?>, ClientMetricsListener> listeners = new IdentityHashMap<Class<?>, ClientMetricsListener>();
    
    public CompositeClientMetricsListener(List<ClientMetricsListener> factories) {
        for (ClientMetricsListener listener : factories) {
            this.listeners.put(listener.getClass(), listener);
        }
    }
    
    @Override
    public void onEvent(ClientEvent event, long duration, TimeUnit timeUnit, Throwable throwable, Object value) {
        for (ClientMetricsListener listener : listeners.values()) {
            listener.onEvent(event, duration, timeUnit, throwable, value);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getMetrics(Class<T> type) {
        return (T)listeners.get(type);
    }
}
