package SQLtools;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import common.RelUtil;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 * Extracts information from a query given as SQL string.
 * 
 * @author immanueltrummer
 *
 */
public class QueryInfo {
	/**
	 * Number of tables used in query.
	 */
	public final int nrTables;
	/**
	 * Relation representing all joined tables.
	 */
	public final BitSet allTables;
	/**
	 * Maps (integer) table ID to table alias.
	 */
	public final Map<Integer, String> tableIDtoAlias;
	/**
	 * Maps table alias to integer ID.
	 */
	public final Map<String, Integer> tableAliasToID;
	/**
	 * Maps table alias to actual table name.
	 */
	public final Map<String, String> tableAliasToName;
	/**
	 * Meta-data about the query predicates.
	 */
	public final List<PredInfo> predicates;
	/**
	 * Maps each table alias to a set of column names
	 * that are referenced in query predicates.
	 */
	public final Map<String, Set<String>> aliasToPredColumns;
	
	public QueryInfo(String sql) throws Exception {
		System.out.println("QueryInfo(" + sql + ")");
		// Parse SQL query into tree
		Statement sqlStatement = CCJSqlParserUtil.parse(sql);
		// Extract tables in query with their aliases
		TableExtractor tableExtractor = new TableExtractor();
		sqlStatement.accept(tableExtractor);
		this.tableAliasToName = tableExtractor.tableAliasToName;
		// Extract and count table aliases
		List<String> tableAliases = new ArrayList<String>();
		tableAliases.addAll(tableAliasToName.keySet());
		this.nrTables = tableAliases.size();
		// Generate relation representing all tables
		this.allTables = new BitSet();
		for (int tableCtr=0; tableCtr<nrTables; ++tableCtr) {
			allTables.set(tableCtr);
		}
		// Assign integer IDs to all tables/aliases
		this.tableIDtoAlias = new HashMap<Integer, String>();
		this.tableAliasToID = new HashMap<String, Integer>();
		int tableCtr = 0;
		for (String tableAlias : tableAliases) {
			tableAliasToID.put(tableAlias, tableCtr);
			tableIDtoAlias.put(tableCtr, tableAlias);
			++tableCtr;
		}
		// Debugging output
		System.out.println("Alias to name:\t" + tableAliasToName.toString());
		System.out.println("Table aliases:\t" + tableAliases.toString());
		System.out.println("Alias to ID:\t" + tableAliasToID.toString());
		System.out.println("ID to Alias:\t" + tableIDtoAlias.toString());
		// Extract predicates from where clause
		PredicateExtractor predExtractor = new PredicateExtractor(tableAliasToID);
		sqlStatement.accept(predExtractor);
		predicates = new ArrayList<PredInfo>();
		while (!predExtractor.predStack.isEmpty()) {
			PredInfo predInfo = predExtractor.predStack.pop();
			if (predInfo instanceof CompositePredInfo) {
				CompositePredInfo compPredInfo = (CompositePredInfo)predInfo;
				if (compPredInfo.connector == PredConnector.AND) {
					predExtractor.predStack.add(compPredInfo.pred1);
					predExtractor.predStack.add(compPredInfo.pred2);
				} else {
					predicates.add(predInfo);
				}
			} else {
				predicates.add(predInfo);
			}
		}
		System.out.println("Independent preds:\t" + predicates.toString());
		// Assign table aliases to columns mentioned in predicates
		aliasToPredColumns = predExtractor.aliasToPredCols;
		System.out.println("Alias columns:\t" + aliasToPredColumns.toString());
	}
	/**
	 * Returns the list of all predicates that are applicable
	 * for the given relation.
	 * 
	 * @param relation	target relation
	 * @return			a list of applicable predicates
	 */
	public List<PredInfo> applicablePreds(BitSet relation) {
		List<PredInfo> predList = new ArrayList<PredInfo>();
		for (PredInfo pred : predicates) {
			if (RelUtil.isSubset(pred.tableIDs, relation)) {
				predList.add(pred);
			}
		}
		return predList;
	}
}
