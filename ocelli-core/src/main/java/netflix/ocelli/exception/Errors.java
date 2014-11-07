package netflix.ocelli.exception;

import netflix.ocelli.RetryableErrorPolicy;

public class Errors {
    public static RetryableErrorPolicy any() {
        return new RetryableErrorPolicy() {
            @Override
            public Boolean call(Throwable t1) {
                return true;
            }
        };
    }
}
