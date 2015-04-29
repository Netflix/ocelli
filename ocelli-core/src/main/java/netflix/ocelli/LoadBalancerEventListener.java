package netflix.ocelli;

import java.util.concurrent.TimeUnit;

public interface LoadBalancerEventListener <E extends LoadBalancerEvent<?>> {

    int NO_DURATION = -1;
    Object NO_VALUE = null;
    Throwable NO_ERROR = null;
    TimeUnit NO_TIME_UNIT = null;

    /**
     * Event callback for any {@link MetricsEvent}. The parameters passed are all the contextual information possible for
     * any event. There presence or absence will depend on the type of event.
     *
     * @param event Event for which this callback has been invoked. This will never be {@code null}
     * @param duration If the passed event is {@link MetricsEvent#isTimed()} then the actual duration, else
     * {@link #NO_DURATION}
     * @param timeUnit The time unit for the duration, if exists, else {@link #NO_TIME_UNIT}
     * @param throwable If the passed event is {@link MetricsEvent#isError()} then the cause of the error, else
     * {@link #NO_ERROR}
     * @param value If the passed event requires custom object to be passed, then that object, else {@link #NO_VALUE}
     */
    void onEvent(E event, long duration, TimeUnit timeUnit, Throwable throwable, Object value);

    /**
     * Marks the end of all event callbacks. No methods on this listener will ever be called once this method is called.
     */
    void onCompleted();

    /**
     * A callback when this listener is subscribed to a {@link MetricEventsPublisher}.
     */
    void onSubscribe();
}
