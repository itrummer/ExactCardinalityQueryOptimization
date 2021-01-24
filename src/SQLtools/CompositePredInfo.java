package SQLtools;

/**
 * Represents meta-data for a composite predicate
 * (i.e., a conjunction or disjunction of other
 * predicates).
 * 
 * @author immanueltrummer
 *
 */
public class CompositePredInfo extends PredInfo {
	/**
	 * The two component predicates.
	 */
	public final PredInfo pred1, pred2;
	/**
	 * Logical connector between component predicates.
	 */
	public final PredConnector connector;
	/**
	 * Initializes the predicate, unions the table
	 * sets referred to in the two components
	 * predicates.
	 * 
	 * @param connector	connecting SQL keyword between component predicates
	 * @param pred1			first component predicate
	 * @param pred2			second component predicate
	 */
	public CompositePredInfo(PredConnector connector, 
			PredInfo pred1, PredInfo pred2) {
		super("(" + pred1.sql + " " + connector.toString() + 
				" " + pred2.sql + ")");
		this.tableIDs.or(pred1.tableIDs);
		this.tableIDs.or(pred2.tableIDs);
		this.pred1 = pred1;
		this.pred2 = pred2;
		this.connector = connector;
		this.columns.addAll(pred1.columns);
		this.columns.addAll(pred2.columns);
	}
	@Override
	public String toString() {
		return "CompPred(" + connector.toString() + ", " + tableIDs.toString() + 
				", " + pred1.toString() + ", " + pred2.toString() + ")";
	}
}
