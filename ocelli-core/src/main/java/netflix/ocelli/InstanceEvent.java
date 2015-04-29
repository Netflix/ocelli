package netflix.ocelli;

@SuppressWarnings("rawtypes")
public class InstanceEvent<T extends Enum> extends AbstractLoadBalancerEvent<T> {

    public enum EventType implements MetricEventType {

        /* Connection specific events. */
        ExecutionSuccess(true, false, Void.class),
        ExecutionFailed(true, true, Void.class),

        ;

        private final boolean isTimed;
        private final boolean isError;
        private final Class<?> optionalDataType;

        EventType(boolean isTimed, boolean isError, Class<?> optionalDataType) {
            this.isTimed = isTimed;
            this.isError = isError;
            this.optionalDataType = optionalDataType;
        }

        @Override
        public boolean isTimed() {
            return isTimed;
        }

        @Override
        public boolean isError() {
            return isError;
        }

        @Override
        public Class<?> getOptionalDataType() {
            return optionalDataType;
        }
    }

    public static final InstanceEvent<EventType> EXECUTION_SUCCESS = from(EventType.ExecutionSuccess);
    public static final InstanceEvent<EventType> EXECUTION_FAILED  = from(EventType.ExecutionFailed);

    /*Always refer to as constants*/protected InstanceEvent(T name, boolean isTimed, boolean isError) {
        super(name, isTimed, isError);
    }

    private static InstanceEvent<EventType> from(EventType type) {
        return new InstanceEvent<EventType>(type, type.isTimed(), type.isError());
    }
}