package SQLtools;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import common.RelUtil;
import common.plans.CompositePlan;
import common.plans.LeafPlan;
import common.plans.ProbePlan;
import net.sf.jsqlparser.schema.Column;

/**
 * Helper methods for formulating SQL queries.
 * 
 * @author immanueltrummer
 *
 */
public class SQLgenerator {
	/**
	 * Information about the original query -
	 * generated queries refer to parts of
	 * this query.
	 */
	public final QueryInfo queryInfo;
	/**
	 * Initializes generator for one original query.
	 * 
	 * @param queryInfo	original input query
	 */
	public SQLgenerator(QueryInfo queryInfo) {
		this.queryInfo = queryInfo;
	}
	/**
	 * Returns an SQL query counting the number of
	 * tuples in the input relation.
	 * 
	 * @param relation	a table set
	 * @return			a query requesting its cardinality
	 */
	public String countQuery(BitSet relation) {
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("SELECT Count(*) FROM ");
		// Generate FROM clause
		//System.out.println("Generating FROM clause");
		List<String> fromItems = new ArrayList<String>();
		for (int tableID=relation.nextSetBit(0); tableID>=0; 
				tableID=relation.nextSetBit(tableID + 1)) {
			//System.out.println("tableID:\t" + tableID);
			String alias = queryInfo.tableIDtoAlias.get(tableID);
			String tableName = queryInfo.tableAliasToName.get(alias);
			fromItems.add(tableName + " AS " + alias);
		}
		String fromClause = String.join(", ", fromItems);
		queryBuilder.append(fromClause);
		// Generate WHERE clause
		//System.out.println("Generating WHERE clause");
		List<String> whereItems = new ArrayList<String>();
		for (PredInfo pred : queryInfo.predicates) {
			if (RelUtil.isSubset(pred.tableIDs, relation)) {
				whereItems.add(pred.sql);
			}
		}
		if (!whereItems.isEmpty()) {
			queryBuilder.append(" WHERE ");
			String whereClause = String.join(" AND ", whereItems);
			queryBuilder.append(whereClause);			
		}
		queryBuilder.append(";");
		return queryBuilder.toString();
	}
	/**
	 * Generates a query selecting a specified
	 * number of null values.
	 * 
	 * @param nrColumns	number of result columns
	 * @return			SQL string representing query
	 */
	String nullColumns(int nrColumns) {
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("SELECT ");
		boolean first = true;
		for (int colCtr=0; colCtr<nrColumns; ++colCtr) {
			queryBuilder.append(first?"":",");
			queryBuilder.append("NULL");
			first = false;
		}
		return queryBuilder.toString();
	}
	/**
	 * Generates a query to retrieve the cardinality of a specific
	 * result via a specified join order - limit clauses ensure that
	 * no intermediate result generated during query processing
	 * exceeds the given bounds.
	 * 
	 * @param queryInfo			meta-data about input query
	 * @param plan				specifies join order
	 * @param limit				limit enforced on intermediate result sizes
	 * @param ignoreBaseTables	whether to suppress limit on base tables
	 * @return					an SQL query realizing the specified plan
	 * @throws Exception
	 */
	public String safeProbeQuery(QueryInfo queryInfo, ProbePlan plan, 
			int limit, boolean ignoreBaseTables) throws Exception {
		List<PredInfo> todoPreds = new ArrayList<PredInfo>();
		todoPreds.addAll(queryInfo.predicates);
		return safeProbeQueryRec(plan, limit, 
				ignoreBaseTables, todoPreds);
	}
	/**
	 * Returns set of columns appearing in unary predicates
	 * on the given table.
	 * 
	 * @param queryInfo	meta-data about query predicates
	 * @param table		extract columns from unary predicates on that table
	 * @return			set of column names from unary predicates
	 */
	/*
	Set<String> unaryCols(QueryInfo queryInfo, int table) {
		Set<String> cols = new HashSet<String>();
		// Iterate over query predicates
		for (PredInfo pred : queryInfo.predicates) {
			// Check for unary predicate
			if (pred.tableIDs.size() == 1) {
				// Check for table
				if (pred.tableIDs.get(table)) {
					for (String alias : pred.aliasToCols.keySet()) {
						cols.addAll(pred.aliasToCols.get(alias));
					}
				}
			}
		}
		return cols;
	}
	*/
	/**
	 * Returns a mapping from table IDs to all columns 
	 * that appear in join predicates connecting tables
	 * in a subset relation to tables in a second relation.
	 * 
	 * @param queryInfo		meta-data about query predicates
	 * @param subRel			relation with subset of tables
	 * @param rel			relation with superset of tables
	 * @return				list of columns in connecting join predicates
	 */
	/*
	Map<Integer, Set<String>> joinCols(QueryInfo queryInfo, 
			BitSet subRel, BitSet rel) {
		Map<Integer, Set<String>> tableToCols = 
				new HashMap<Integer, Set<String>>();
		// Iterate over query predicates
		for (PredInfo pred : queryInfo.predicates) {
			// Check for join predicates
			if (pred.tableIDs.cardinality() > 1) {
				System.out.println("Final rel:\t" + rel.toString());
				System.out.println("Current rel:\t" + subRel.toString());
				System.out.println("Join predicate: " + pred.toString());
				// Check whether predicate is applied in final result
				if (RelUtil.isSubset(pred.tableIDs, rel)) {
					System.out.println("applies to final result");
					// Check whether predicate not evaluated before
					if (!RelUtil.isSubset(pred.tableIDs, subRel)) {
						System.out.println("cannot apply to current result");
						BitSet overlap = new BitSet();
						overlap.or(pred.tableIDs);
						overlap.and(subRel);
						System.out.println(overlap.toString());
						for (int table=overlap.nextSetBit(0); table>=0; 
								table=overlap.nextSetBit(table+1)) {
							String alias = queryInfo.tableIDtoAlias.get(table);
							Set<String> aliasCols = pred.aliasToCols.get(alias);
							System.out.println("Alias:\t" + alias);
							System.out.println("Cols:\t" + aliasCols);
							tableToCols.put(table, aliasCols);
						}
					}
				}
			}
		}
		return tableToCols;
	}
	*/
	/**
	 * Returns a list of selectors for the query WHERE
	 * clause. Selectors are generated for columns of
	 * applicable predicates. The corresponding 
	 * predicates are removed from the todo list. 
	 * 
	 * @param queryInfo		meta-data about input query
	 * @param rel			relation in FROM clause
	 * @param todoPreds		un-evaluated predicates
	 * @return				a list of items for the SELECT clause
	 */
	Set<String> selectors(QueryInfo queryInfo, BitSet rel, 
			List<PredInfo> todoPreds) {
		Set<String> selectors = new HashSet<String>();
		boolean isLeaf = rel.cardinality() == 1;
		Iterator<PredInfo> todoPredsIter = todoPreds.iterator();
		while (todoPredsIter.hasNext()) {
			PredInfo pred = todoPredsIter.next();
			for (Column col : pred.columns) {
				String alias = col.getTable().getName();
				Integer tableID = queryInfo.tableAliasToID.get(alias);
				if (rel.get(tableID)) {
					String colName = col.getColumnName();
					String globalColName = alias + "_" + colName;
					String selector = isLeaf ? colName + " AS " + 
							globalColName:globalColName;
					selectors.add(selector);					
				}
			}
			if (pred.applicable(rel)) {
				todoPredsIter.remove();
			}
		}
		// Treat special case: no un-evaluated predicates left
		if (selectors.isEmpty()) {
			selectors.add("*");
		}
		return selectors;
	}
	/**
	 * Generates a query to retrieve the cardinality of a specific
	 * result via a specified join order - limit clauses ensure that
	 * no intermediate result generated during query processing
	 * exceeds the given bounds. Used for recursive calls.
	 * 
	 * @param plan				specifies join order
	 * @param limit				limit enforced on intermediate result sizes
	 * @param ignoreBaseTables	whether to suppress limit on base tables
	 * @param todoPreds			predicates that were not yet applied
	 * @return					an SQL query realizing the specified plan
	 */
	String safeProbeQueryRec(ProbePlan plan, int limit, 
			boolean ignoreBaseTables, List<PredInfo> todoPreds) 
					throws Exception {
		StringBuilder queryBuilder = new StringBuilder();
		if (plan instanceof LeafPlan) {
			LeafPlan leafPlan = (LeafPlan)plan;
			int tableID = leafPlan.table;
			String tableAlias = queryInfo.tableIDtoAlias.get(tableID);
			String tableName = queryInfo.tableAliasToName.get(tableAlias);
			// Add WHERE clause
			Set<String> selectors = selectors(
					queryInfo, leafPlan.resultRel, todoPreds);
			queryBuilder.append("SELECT ");
			queryBuilder.append(String.join(", ", selectors));
			// Add FROM clause
			queryBuilder.append(" FROM ");
			queryBuilder.append(tableName);
			queryBuilder.append(" AS ");
			queryBuilder.append(tableAlias);
			// Insert all applicable predicates
			BitSet tableSet = new BitSet();
			tableSet.set(tableID);
			List<PredInfo> applicablePreds = queryInfo.applicablePreds(tableSet);
			if (!applicablePreds.isEmpty()) {
				queryBuilder.append(" WHERE TRUE ");
				for (PredInfo pred : applicablePreds) {
					queryBuilder.append(" AND ");
					queryBuilder.append(pred.sql);
				}
			}
			// Add limit clause unless we ignore base tables
			if (!ignoreBaseTables) {
				queryBuilder.append(" LIMIT ");
				queryBuilder.append(limit);				
			}
		} else {
			// Plan joins result of two prior plans -
			// extract required information.
			CompositePlan compositePlan = (CompositePlan)plan;
			BitSet resultRel = compositePlan.resultRel;
			ProbePlan plan1 = compositePlan.leftPlan;
			ProbePlan plan2 = compositePlan.rightPlan;
			BitSet plan1rel = plan1.resultRel;
			BitSet plan2rel = plan2.resultRel;
			String plan1SQL = safeProbeQueryRec(plan1, 
					limit, ignoreBaseTables, todoPreds);
			String plan2SQL = safeProbeQueryRec(plan2, 
					limit, ignoreBaseTables, todoPreds);
			// Get selectors (important: needs to happen
			// after recursive invocations!).
			Set<String> selectors = selectors(
					queryInfo, resultRel, todoPreds);
			// Add select clause
			queryBuilder.append("SELECT ");
			queryBuilder.append(String.join(", ", selectors));
			//queryBuilder.append("*");
			/*
			// Collect columns required for upstream processing
			if (plan.resultRel.equals(finalRel)) {
				queryBuilder.append("SELECT *");				
			} else {
				// Collect relevant columns
				Map<Integer, Set<String>> cols = joinCols(
						queryInfo, resultRel, finalRel);
				List<String> selectors = new ArrayList<String>();
				for (Entry<Integer, Set<String>> entry : cols.entrySet()) {
					Integer tableID = entry.getKey();
					Set<String> tableCols = entry.getValue();
					String alias = queryInfo.tableIDtoAlias.get(tableID);
					for (String col : tableCols) {
						// Selector name uses table alias as prefix
						String selector = alias + "_" + col;
						selectors.add(selector);
					}
				}
				// Generate SELECT clause
				queryBuilder.append("SELECT ");
				queryBuilder.append(String.join(", ", selectors)); 
			}
			*/
			queryBuilder.append(" FROM (");
			queryBuilder.append(plan1SQL);
			queryBuilder.append(") as L, (");
			queryBuilder.append(plan2SQL);
			queryBuilder.append(") as R ");
			// Treat predicates which are applicable
			// for join result but not for operands.
			List<PredInfo> preds = queryInfo.applicablePreds(resultRel);
			preds.removeAll(queryInfo.applicablePreds(plan1rel));
			preds.removeAll(queryInfo.applicablePreds(plan2rel));
			if (!preds.isEmpty()) {
				queryBuilder.append(" WHERE TRUE ");
				for (PredInfo pred : preds) {
					queryBuilder.append(" AND ");
					queryBuilder.append(pred.sql.replace(".", "_"));
				}
			}
			// Add limit clause
			queryBuilder.append(" LIMIT ");
			queryBuilder.append(limit);
		}
		return queryBuilder.toString();
	}
}
