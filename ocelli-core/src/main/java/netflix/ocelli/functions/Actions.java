package netflix.ocelli.functions;

import java.util.concurrent.atomic.AtomicBoolean;

import rx.functions.Action0;

public abstract class Actions {
    public static Action0 once(final Action0 delegate) {
        return new Action0() {
            private AtomicBoolean called = new AtomicBoolean(false);
            @Override
            public void call() {
                if (called.compareAndSet(false, true)) {
                    delegate.call();
                }
            }
        };
    }
}
