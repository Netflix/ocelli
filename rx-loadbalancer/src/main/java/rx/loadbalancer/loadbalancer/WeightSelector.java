package rx.loadbalancer.loadbalancer;

import rx.functions.Func2;

/**
 * Plugable function for selecting an index from an array of weights
 * 
 * Integer[] - Array of accumulated weights with each cell corresponding to a Client
 * Integer - Maximum number of weights (not clients)
 * Integer - Index of selected cell 
 * 
 * @author elandau
 *
 */
public interface WeightSelector extends Func2<Integer[], Integer, Integer> {

}
