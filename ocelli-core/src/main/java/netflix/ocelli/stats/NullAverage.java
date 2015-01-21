package netflix.ocelli.stats;

import rx.functions.Func0;

public class NullAverage implements Average {
    public static Func0<Average> factory() {
        return new Func0<Average>() {
            @Override
            public Average call() {
                return new NullAverage();
            }
        };
    }
    
    @Override
    public void add(int sample) {
    }

    @Override
    public double get() {
        return 0;
    }

    @Override
    public void reset() {
    }

}
