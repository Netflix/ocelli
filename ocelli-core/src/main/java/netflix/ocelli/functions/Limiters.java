package netflix.ocelli.functions;

import netflix.ocelli.stats.ExponentialAverage;
import rx.functions.Func1;

public abstract class Limiters {
    /**
     * Guard against excessive backup requests using exponential moving average
     * on a per sample basis which is incremented by 1 for each request and by 0 
     * for each backup request.  A backup request is allowed as long as the average
     * is able the expected percentile of primary requests to backup requests.
     * 
     * Note that this implementation favors simplicity over accuracy and has 
     * many drawbacks.
     * 1.  Backup requests are per request and not tied to any time window
     * 2.  I have yet to determine an equation that selects the proper window
     *     for the requested ratio so that the updated exponential moving average
     *     allows the ratio number of requests.
     * @param ratio
     * @param window
     * @return
     */
    public static Func1<Boolean, Boolean> exponential(final double ratio, final int window) {
        return new Func1<Boolean, Boolean>() {
            private ExponentialAverage exp = new ExponentialAverage(window, 0);
            @Override
            public Boolean call(Boolean isPrimary) {
                if (isPrimary) {
                    exp.add(1);
                    return true;
                }
                if (exp.get() > ratio) {
                    exp.add(0);
                    return true;
                }
                return false;
            }
        };
    }
}
