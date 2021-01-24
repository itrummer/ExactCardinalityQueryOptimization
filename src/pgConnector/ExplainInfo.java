package pgConnector;

import java.sql.ResultSet;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import pgConnector.json.JSONexplainResult;
import pgConnector.json.JSONpgPlan;

/**
 * Contains meta-data about the plan chosen by the
 * Postgres optimizer for a specific query.
 * 
 * @author immanueltrummer
 *
 */
public class ExplainInfo {
	/**
	 * The query whose plan is analyzed.
	 */
	public final String query;
	/**
	 * Maps table aliases to IDs.
	 */
	public final Map<String, Integer> tableAliasToID;
	/**
	 * Execution cost as estimated by the Postgres optimizer.
	 */
	public final double cost;
	/**
	 * Number of rows estimated by the Postgres optimizer.
	 */
	public final double card;
	/**
	 * Intermediate results whose cardinality will be revealed
	 * by executing this plan.
	 */
	public final HashSet<BitSet> covered;
	/**
	 * Uses Postgres explain on a given query in order to
	 * obtain information about the corresponding Postgres plan.
	 * 
	 * @param query				query for which to get plan
	 * @param tableAliasToID		maps table aliases to table IDs
	 * @param pgConnector		enables database queries
	 * @throws Exception
	 */
	public ExplainInfo(String query, Map<String, Integer> tableAliasToID,
			PgConnector pgConnector) throws Exception {
		//System.out.println("Entry PlanInfo constructor");
		this.query = query;
		this.tableAliasToID = tableAliasToID;
		// Issue query to Postgres
		//PgConnector pgConnector = new PgConnector("imdbload");
		String explainQuery = "EXPLAIN (FORMAT JSON) " + query;
		//System.out.println("About to query for plan");
		ResultSet result = pgConnector.query(explainQuery, -1);
		//System.out.println("About to query for plan");
		// Retrieve result string
		StringBuffer resultBuffer = new StringBuffer();
		while (result.next()) {
			resultBuffer.append(result.getString(1));
		}
		String resultString = resultBuffer.toString();
		//System.out.println("Obtained query result");
		//System.out.println(resultString);
		// Post-processing of explain result for JSON parsing
		resultString = resultString.replace(" ", "");
		resultString = resultString.toLowerCase();
		resultString = resultString.substring(1, resultString.length() - 1);
		// Extract JSON from result string
		ObjectMapper objectMapper = new ObjectMapper();
		JSONexplainResult explainResult = objectMapper.readValue(
				resultString, JSONexplainResult.class);
		// Extract information from plan
		cost = explainResult.getPlan().getTotalcost();
		card = explainResult.getPlan().getPlanrows();
		covered = new HashSet<BitSet>();
		//System.out.println("About to collect relations");
		collectRelations(explainResult.getPlan());
	}
	/**
	 * Collect relations that are generated while
	 * executing the given plan. This method is
	 * recursive.
	 * 
	 * @param plan	plan in which we collect relations	
	 * @return		plan result relation as table ID set
	 */
	BitSet collectRelations(JSONpgPlan plan) {
		BitSet relation = new BitSet();
		// Is this a leaf node?
		String alias = plan.getAlias();
		//System.out.println("Alias:" + alias + "|");
		if (alias != null) {
			Integer tableID = tableAliasToID.get(alias);
			relation.set(tableID);
			covered.add(relation);
		} else {
			// No leaf node - must have child plan nodes
			for (JSONpgPlan childPlan : plan.getPlans()) {
				relation.or(collectRelations(childPlan));
			}
			covered.add(relation);
		}
		return relation;
	}
	@Override
	public String toString() {
		return "Cost:\t" + cost + "\tCovered:\t" + covered + "\tQuery:\t" + query;
	}
}
