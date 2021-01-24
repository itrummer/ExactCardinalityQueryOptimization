package optimizers.safe;

/**
 * Describes cost model to use for optimization.
 * 
 * @author immanueltrummer
 *
 */
public enum CostModel {
	LOWER_BOUNDS,	// use lower cardinality bounds for optimistic cost
	UPPER_BOUNDS,	// use upper cardinality bounds for pessimistic cost
	VERIFIABILITY,	// maximize probability of successful verification
	NR_VERIFIABLE,	// maximizes number of relations that could verify
	BEST_GUESS,		// use current best guess cardinality
	SAFE_GUESS		// binary cost metric: guess cannot exceed limit
}
