package netflix.ocelli.rxnetty.protocol;

import netflix.ocelli.rxnetty.internal.HostConnectionProvider;

import java.util.Comparator;

/**
 * A comparator for {@link WeightAware}
 */
public class WeightComparator<W, R> implements Comparator<HostConnectionProvider<W, R>> {
    @Override
    public int compare(HostConnectionProvider<W, R> cp1, HostConnectionProvider<W, R> cp2) {
        WeightAware wa1 = (WeightAware) cp1.getEventsListener();
        WeightAware wa2 = (WeightAware) cp2.getEventsListener();
        return wa1.getWeight() > wa2.getWeight() ? 1 : -1;
    }
}
