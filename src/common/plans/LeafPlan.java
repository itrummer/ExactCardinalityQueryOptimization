package common.plans;

import java.util.BitSet;

import SQLtools.QueryInfo;

/**
 * Represents access to a single base table.
 * 
 * @author immanueltrummer
 */
public class LeafPlan extends ProbePlan {
	/**
	 * The table accessed by this leaf plan.
	 */
	public final int table;
	/**
	 * Initializes a plan accessing a single table.
	 * 
	 * @param queryInfo		meta-data on input query
	 * @param table			the accessed table
	 * @param cost			tale access cost
	 */
	public LeafPlan(QueryInfo queryInfo, int table, double cost) {
		super(singletonRel(table), cost, nrColumns(queryInfo, table));
		this.table = table;
	}
	/**
	 * Returns a BitSet representing a singleton relation.
	 * 
	 * @param table	single table in relation
	 * @return		a BitSet representing singleton relation
	 */
	static BitSet singletonRel(int table) {
		BitSet rel = new BitSet();
		rel.set(table);
		return rel;
	}
	/**
	 * Extracts number of columns selected for given
	 * table from query info object.
	 * 
	 * @param queryInfo	meta-data about input query
	 * @param table		we retrieve columns associated with that table
	 * @return			number of columns associated with that table
	 */
	static int nrColumns(QueryInfo queryInfo, int table) {
		String alias = queryInfo.tableIDtoAlias.get(table);
		return queryInfo.aliasToPredColumns.get(alias).size();
	}
	@Override
	public boolean findRel(BitSet rel) {
		return this.resultRel.equals(rel);
	}
}
