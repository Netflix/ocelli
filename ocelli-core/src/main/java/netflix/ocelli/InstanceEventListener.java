package netflix.ocelli;

import java.util.concurrent.TimeUnit;

public abstract class InstanceEventListener implements LoadBalancerEventListener<InstanceEvent<?>> {

    @Override
    public void onEvent(InstanceEvent<?> event, long duration, TimeUnit timeUnit, Throwable throwable, Object value) {
        switch ((InstanceEvent.EventType) event.getType()) {
            case ExecutionSuccess:
                onExecutionSuccess(duration, timeUnit);
                break;
            case ExecutionFailed:
                onExecutionFailed(duration, timeUnit, throwable);
                break;
        }
    }

    protected void onExecutionFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        // No Op
    }

    protected void onExecutionSuccess(long duration, TimeUnit timeUnit) {
        // No Op
    }

    @Override
    public void onCompleted() {
        // No Op
    }

    @Override
    public void onSubscribe() {
        // No Op
    }
}
