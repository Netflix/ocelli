package rx.loadbalancer.selectors;

import java.util.concurrent.TimeUnit;

import rx.functions.Func1;

public class Delays {
    public static Func1<Integer, Long> fixed(final int delay, final TimeUnit units) {
        return new Func1<Integer, Long>() {
            @Override
            public Long call(Integer t1) {
                return TimeUnit.MILLISECONDS.convert(delay, units);
            }
        };
    }

    public static Func1<Integer, Long> linear(final int delay, final TimeUnit units) {
        return new Func1<Integer, Long>() {
            @Override
            public Long call(Integer counter) {
                return counter * TimeUnit.MILLISECONDS.convert(delay, units);
            }
        };
    }

    public static Func1<Integer, Long> exp(final int step, final TimeUnit units) {
        return new Func1<Integer, Long>() {
            @Override
            public Long call(Integer count) {
                if (count < 0) 
                    count = 0;
                else if (count > 10) 
                    count = 10;
                return (1 << count) * TimeUnit.MILLISECONDS.convert(step, units);
            }
        };
    }
}
