package pgConnector.json;

/**
 * Represents an explain query result in JSON format.
 * 
 * @author immanueltrummer
 *
 */
public class JSONexplainResult {
	private JSONpgPlan plan;

	public JSONpgPlan getPlan() {
		return plan;
	}

	public void setPlan(JSONpgPlan plan) {
		this.plan = plan;
	}
}
