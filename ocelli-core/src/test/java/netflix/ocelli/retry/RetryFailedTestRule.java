package netflix.ocelli.retry;

import java.util.Random;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RetryFailedTestRule implements TestRule {
    private int attemptNumber;

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
                Random prng = new Random();
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

                        // sleep a bit to minimize failures due to tests with shared fixtures running at the same time
                        Thread.sleep(prng.nextInt(12000));
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
