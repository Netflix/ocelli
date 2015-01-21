package netflix.ocelli.functions;

import org.junit.Test;

import rx.functions.Func1;

public class GuardsTest {
    @Test
    public void shouldRejectAfter100Percent() {
        Func1<Boolean, Boolean> guard = Limiters.exponential(0.90, 30);
        int discarded = 0;
        for (int i = 0; i < 100; i++) {
            guard.call(true);
            
            if (!guard.call(false)) {
                discarded++;
            }
        }
        System.out.println("Discarded : " + discarded);
    }
    
    @Test
    public void shouldRejectAfter90Percent() {
        Func1<Boolean, Boolean> guard = Limiters.exponential(0.90, 30);
        
        int discarded = 0;
        for (int i = 0; i < 100; i++) {
            guard.call(true);
            if (i % 5 == 0) {
                if (!guard.call(false)) {
                    discarded++;
                }
            }
        }
        
        System.out.println("Discarded : " + discarded);
    }
}
