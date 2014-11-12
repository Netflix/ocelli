package netflix.ocelli.retry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RetryFailedTestRule implements TestRule {
    private int attemptNumber;

    @Target({ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Retry {
        int value();
    }
    
    public RetryFailedTestRule() {
        this.attemptNumber = 0;
    }

    public Statement apply(final Statement base, final Description description) {
        Retry retry = description.getAnnotation(Retry.class);
        final int retryCount = retry == null ? 1 : retry.value();
        
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Throwable caughtThrowable = null;

                for (attemptNumber = 0; attemptNumber < retryCount; ++attemptNumber) {
                    try {
                        base.evaluate();

                        System.err.println(description.getDisplayName() + ": attempt number " + attemptNumber + " succeeded");

                        return;
                    } catch (Throwable t) {
                        caughtThrowable = t;

                        System.err.println(description.getDisplayName() + ": attempt number " + attemptNumber + " failed:");
                        System.err.println(t.toString());
                    }
                }

                System.err.println(description.getDisplayName() + ": giving up after " + retryCount + " failures.");

                throw caughtThrowable;
            }
        };
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }
}
