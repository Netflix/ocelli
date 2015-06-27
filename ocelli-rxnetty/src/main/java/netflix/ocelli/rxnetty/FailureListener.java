package netflix.ocelli.rxnetty;

import rx.Scheduler;

import java.util.concurrent.TimeUnit;

/**
 * A contract for taking actions upon detecting an unhealthy host. A failure listener instance is always associated with
 * a unique host, so any action taken on this listener will directly be applied to the associated host.
 */
public interface FailureListener {

    /**
     * This action will remove the host associated with this listener from the load balancing pool.
     */
    void remove();

    /**
     * This action quarantines the host associated with this listener from the load balancing pool, for the passed
     * {@code quarantineDuration}. The host will be added back to the load balancing pool after the quarantine duration
     * is elapsed.
     *
     * @param quarantineDuration Duration for keeping the host quarantined.
     * @param timeUnit Time unit for the duration.
     */
    void quarantine(long quarantineDuration, TimeUnit timeUnit);

    /**
     * This action quarantines the host associated with this listener from the load balancing pool, for the passed
     * {@code quarantineDuration}. The host will be added back to the load balancing pool after the quarantine duration
     * is elapsed.
     *
     * @param quarantineDuration Duration for keeping the host quarantined.
     * @param timeUnit Time unit for the duration.
     * @param timerScheduler Scheduler to be used for the quarantine duration timer.
     */
    void quarantine(long quarantineDuration, TimeUnit timeUnit, Scheduler timerScheduler);
}
