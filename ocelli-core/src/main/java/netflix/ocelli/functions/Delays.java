package netflix.ocelli.functions;

import java.util.concurrent.TimeUnit;

import rx.functions.Func1;

public abstract class Delays {
    public static Func1<Integer, Long> fixed(final long delay, final TimeUnit units) {
        return new Func1<Integer, Long>() {
            @Override
            public Long call(Integer t1) {
                return TimeUnit.MILLISECONDS.convert(delay, units);
            }
        };
    }

    public static Func1<Integer, Long> linear(final long delay, final TimeUnit units) {
        return new Func1<Integer, Long>() {
            @Override
            public Long call(Integer counter) {
                return counter * TimeUnit.MILLISECONDS.convert(delay, units);
            }
        };
    }

    public static Func1<Integer, Long> exp(final long step, final TimeUnit units) {
        return new Func1<Integer, Long>() {
            @Override
            public Long call(Integer count) {
                if (count < 0) 
                    count = 0;
                else if (count > 30) 
                    count = 30;
                return (1 << count) * TimeUnit.MILLISECONDS.convert(step, units);
            }
        };
    }
    
    public static Func1<Integer, Long> boundedExp(final long step, final long max, final TimeUnit units) {
        return new Func1<Integer, Long>() {
            @Override
            public Long call(Integer count) {
                if (count < 0) 
                    count = 0;
                else if (count > 30) 
                    count = 30;
                long delay = (1 << count) * TimeUnit.MILLISECONDS.convert(step, units);
                if (delay > max) {
                    return max;
                }
                return delay;
            }
        };
    }
}
