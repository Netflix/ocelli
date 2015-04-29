package netflix.ocelli.functions;

import java.util.concurrent.TimeUnit;

import netflix.ocelli.DelayStrategy;

public abstract class Delays {
    public static DelayStrategy fixed(final long delay, final TimeUnit units) {
        return new DelayStrategy() {
            @Override
            public long get(int count) {
                return TimeUnit.MILLISECONDS.convert(delay, units);
            }
        };
    }

    public static DelayStrategy linear(final long delay, final TimeUnit units) {
        return new DelayStrategy() {
            @Override
            public long get(int count) {
                return count * TimeUnit.MILLISECONDS.convert(delay, units);
            }
        };
    }

    public static DelayStrategy exp(final long step, final TimeUnit units) {
        return new DelayStrategy() {
            @Override
            public long get(int count) {
                if (count < 0) 
                    count = 0;
                else if (count > 30) 
                    count = 30;
                return (1 << count) * TimeUnit.MILLISECONDS.convert(step, units);
            }
        };
    }
    
    public static DelayStrategy boundedExp(final long step, final long max, final TimeUnit units) {
        return new DelayStrategy() {
            @Override
            public long get(int count) {
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

    public static DelayStrategy immediate() {
        return new DelayStrategy() {
            @Override
            public long get(int t1) {
                return 0L;
            }
        };
    }
}
