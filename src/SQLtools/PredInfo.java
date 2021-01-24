package SQLtools;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Stack;

import common.RelUtil;
import net.sf.jsqlparser.schema.Column;

/**
 * Captures meta-data about a predicate -
 * this meta-data is for instance used to
 * decide whether the predicate can be
 * evaluated on a specific relation.
 * 
 * @author immanueltrummer
 *
 */
public class PredInfo {
	/**
	 * SQL string representing this predicate.
	 */
	public final String sql;
	/**
	 * Contains the table IDs that the predicate
	 * refers to.
	 */
	public final BitSet tableIDs;
	/**
	 * Columns that the predicate refers to.
	 */
	public final List<Column> columns;
	/**
	 * Initializes the predicate with an empty
	 * table set.
	 * 
	 * @param sql	SQL string representing the predicate
	 */
	PredInfo(String sql) {
		this.sql = sql;
		this.tableIDs = new BitSet();
		this.columns = new ArrayList<Column>();
	}
	/**
	 * Initializes the predicate from a given SQL string,
	 * popping all referenced tables from a given stack.
	 * 
	 * @param sql			SQL string representing predicate
	 * @param tableIDstack	a stack containing table IDs (stack becomes empty)
	 * @param columnsStack	stack with associated columns (becomes empty)
	 */
	public PredInfo(String sql, Stack<Integer> tableIDstack, 
			Stack<Column> columnsStack) {
		this(sql);
		while (!tableIDstack.isEmpty()) {
			Integer tableID = tableIDstack.pop();
			tableIDs.set(tableID);
		}
		while (!columnsStack.isEmpty()) {
			columns.add(columnsStack.pop());
		}
	}
	/**
	 * Returns true iff the predicate can be evaluated
	 * on a given relation.
	 * 
	 * @param rel	relation on which predicate is evaluated
	 * @return		true iff the predicate can be evaluated
	 */
	public boolean applicable(BitSet rel) {
		return RelUtil.isSubset(tableIDs, rel);
	}
	@Override
	public String toString() {
		return "Pred(" + sql + ") referring to " + tableIDs.toString();
	}
}
