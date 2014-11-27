package netflix.ocelli.weighted;

import netflix.ocelli.selectors.ClientsAndWeights;
import rx.functions.Func1;

public class LinearWeightingStrategy<C> implements WeightingStrategy<C> {
    
    private Func1<C, Integer> func;

    public LinearWeightingStrategy(Func1<C, Integer> func) {
        this.func = func;
    }
    
    @Override
    public ClientsAndWeights<C> call(C[] clients) {
        int[] weights = new int[clients.length];
        
        if (clients.length > 0) {
            for (int i = 0; i < clients.length; i++) {
                weights[i] = func.call(clients[i]);
            }
    
            int sum = 0;
            for (int i = 0; i < weights.length; i++) {
                sum += weights[i];
                weights[i] = sum;
            }
        }
        return new ClientsAndWeights<C>(clients, weights);
    }
}
