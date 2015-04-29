package netflix.ocelli;

@SuppressWarnings("rawtypes")
public interface LoadBalancerEvent <T extends Enum> {

    T getType();

    boolean isTimed();

    boolean isError();

    /**
     * This interface is a "best-practice" rather than a contract as a more strongly required contract is for the event
     * type to be an enum.
     */
    interface MetricEventType {

        boolean isTimed();

        boolean isError();

        Class<?> getOptionalDataType();
    }
}