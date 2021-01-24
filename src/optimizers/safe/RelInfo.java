package optimizers.safe;

import java.io.Serializable;

/**
 * Contains meta-data about a relation.
 * 
 * @author immanueltrummer
 *
 */
public class RelInfo implements Serializable {
	/**
	 * Compiler-generated ID.
	 */
	private static final long serialVersionUID = -2575587468683903937L;
	/**
	 * Indicates optimization status of this relation.
	 */
	public RelStatus relStatus = RelStatus.PENDING;
	/**
	 * Lower bound on relation cardinality.
	 */
	public double lowerCardBound = 0;
	/**
	 * Represents current best guess on the cardinality
	 * of this relation - initialized using the Postgres
	 * optimizer and updated as new information becomes
	 * available.
	 */
	public double cardBestGuess = -1;
	/**
	 * Upper bound on relation cardinality.
	 */
	public double upperCardBound = Double.POSITIVE_INFINITY;
	/**
	 * Lower bound on cost of generating this relation.
	 * Cost is calculated according to the C_out cost
	 * metric and this bounds includes the cost
	 * associated with this relation itself.
	 */
	public double generationCostLB = 0;
	/**
	 * Lower bound on the cost of generating the final
	 * result from this relation. This bound excludes
	 * the cost associated with this relation itself.
	 */
	public double completionCostLB = 0;
	/**
	 * Lower bound on cost of any complete plan 
	 * that generates this relation.
	 */
	public double lowerCostBound = 0;
	@Override
	public String toString() {
		return "Status:\t" + relStatus.toString() + 
				"\t\tcardLB:\t" + lowerCardBound +
				"\t\tcardBG:\t" + cardBestGuess +
				"\t\tcardUB:\t" + upperCardBound +
				"\t\tgenCost:\t" + generationCostLB +
				"\t\tcompCost:\t" + completionCostLB +
				"\t\tcostLB:\t" + lowerCostBound;
	}
}
