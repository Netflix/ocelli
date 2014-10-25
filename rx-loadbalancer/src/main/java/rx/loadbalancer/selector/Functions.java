package rx.loadbalancer.selector;

import rx.functions.Func1;

public abstract class Functions {
    public static Func1<Integer, Integer> log() {
        return new Func1<Integer, Integer>() {
            @Override
            public Integer call(Integer t1) {
                return (int)Math.ceil(Math.log(t1));
            }
        };
    }
    
    public static Func1<Integer, Integer> log_log() {
        return new Func1<Integer, Integer>() {
            @Override
            public Integer call(Integer t1) {
                return (int)Math.ceil(Math.log(Math.log(t1)));
            }
        };
    }
    
    public static Func1<Integer, Integer> identity() {
        return new Func1<Integer, Integer>() {
            @Override
            public Integer call(Integer t1) {
                return t1;
            }
        };
    }
    
    public static Func1<Integer, Integer> sqrt() {
        return new Func1<Integer, Integer>() {
            @Override
            public Integer call(Integer t1) {
                return (int)Math.ceil(Math.sqrt((double)t1));
            }
        };
    }

    public static Func1<Integer, Integer> root(final double pow) {
        return new Func1<Integer, Integer>() {
            @Override
            public Integer call(Integer t1) {
                return (int)Math.ceil(Math.pow((double)t1, 1/pow));
            }
        };
    }
}
