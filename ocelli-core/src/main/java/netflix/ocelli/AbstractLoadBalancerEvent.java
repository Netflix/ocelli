package netflix.ocelli;

@SuppressWarnings("rawtypes")
public class AbstractLoadBalancerEvent <T extends Enum> implements LoadBalancerEvent<T> {

    protected final T name;
    protected final boolean isTimed;
    protected final boolean isError;

    protected AbstractLoadBalancerEvent(T name, boolean isTimed, boolean isError) {
        this.isTimed = isTimed;
        this.name = name;
        this.isError = isError;
    }

    @Override
    public T getType() {
        return name;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractLoadBalancerEvent)) {
            return false;
        }

        AbstractLoadBalancerEvent that = (AbstractLoadBalancerEvent) o;

        if (isError != that.isError) {
            return false;
        }
        if (isTimed != that.isTimed) {
            return false;
        }
        if (name != that.name) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (isTimed ? 1 : 0);
        result = 31 * result + (isError ? 1 : 0);
        return result;
    }
}