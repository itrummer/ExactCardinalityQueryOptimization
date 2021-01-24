package common;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import SQLtools.PredInfo;
import SQLtools.QueryInfo;
import common.plans.CompositePlan;
import common.plans.LeafPlan;
import common.plans.ProbePlan;

/**
 * Utility methods that are used by multiple planners.
 * 
 * @author immanueltrummer
 *
 */
public class PlanUtil {
	/**
	 * Contains meta-data extracted from an SQL query.
	 */
	public final QueryInfo queryInfo;
	/**
	 * All relations that a typical query planner
	 * considers for the input query.
	 */
	public final Set<BitSet> allRels;
	/**
	 * All relations considered by a typical planner
	 * and grouped by their cardinality (i.e., the
	 * number of joined tables).
	 */
	public final Map<Integer, Set<BitSet>> relsByCard;
	/**
	 * Contains for each table set forming valid relations
	 * (i.e., relations that avoid Cartesian product joins)
	 * the list of all table subsets forming valid relations.
	 */
	public final Map<BitSet, Set<BitSet>> relToSubsets;
	/**
	 * Maps each relation to a set of valid relations that
	 * join a superset of tables compared to the initial
	 * relation.
	 */
	public final Map<BitSet, Set<BitSet>> relToSupsets;
	/**
	 * Generates plan space for input query.
	 * 
	 * @param queryInfo	information about SQL query
	 */
	public PlanUtil(QueryInfo queryInfo) {
		this.queryInfo = queryInfo;
		this.allRels = generateRels(queryInfo);
		//System.out.println("Generated all rels");
		this.relsByCard = groupRels(allRels, queryInfo.nrTables);
		this.relToSubsets = new HashMap<BitSet, Set<BitSet>>();
		this.relToSupsets = new HashMap<BitSet, Set<BitSet>>();
		decomposeRels(allRels, relToSubsets, relToSupsets);
	}
	/**
	 * Transforms an integer value representing a
	 * table subset into a corresponding BitSet.
	 * 
	 * @param relID	integer representing table set
	 * @return		BitSet representation of table set
	 */
	static BitSet intToRel(int relID) {
		BitSet rel = new BitSet();
		int tableCtr = 0;
		while (relID != 0) {
			if (relID % 2 == 1) {
				rel.set(tableCtr);
			}
			relID = relID/2;
			++tableCtr;
		}
		return rel;
	}
	/**
	 * Returns true iff the given relation would be
	 * considered by a typical query planner which
	 * avoids Cartesian product joins if possible.
	 * 
	 * @param relation	an input relation to verify
	 * @param queryInfo	information on query predicates
	 * @return			true iff the relation should be considered
	 */
	static boolean isValid(BitSet relation, QueryInfo queryInfo) {
		// Check whether all activated tables are reachable
		// from a first table when following predicate links.
		int firstTable = relation.nextSetBit(0);
		BitSet reachable = new BitSet();
		reachable.set(firstTable);
		boolean updated = true;
		while (updated) {
			updated = false;
			for (PredInfo pred : queryInfo.predicates) {
				/*
				System.out.println("Reachable:\t" + reachable.toString());
				System.out.println("Predicate:\t" + pred.tableIDs.toString());
				System.out.println("isSubset:\t" + RelUtil.isSubset(pred.tableIDs, relation));
				System.out.println("intersects:\t" + pred.tableIDs.intersects(reachable));
				*/
				// We assume unary and binary predicates
				if (RelUtil.isSubset(pred.tableIDs, relation) &&
						pred.tableIDs.intersects(reachable)) {
					BitSet beforeOr = (BitSet)reachable.clone();
					reachable.or(pred.tableIDs);
					if (!reachable.equals(beforeOr)) {
						updated = true;						
					}
				}
			}
		}
		return reachable.equals(relation);
	}
	/**
	 * Returns all relations that would be considered
	 * by a typical query planner (excluding Cartesian
	 * product joins).
	 * 
	 * @param queryInfo	information on query predicates
	 * @return			all candidate relations for planner
	 */
	static Set<BitSet> generateRels(QueryInfo queryInfo) {
		Set<BitSet> rels = new HashSet<BitSet>();
		// Iterate over tentative relation subsets
		int maxRelCount = 2 << (queryInfo.nrTables) - 1;
		for (int relID=1; relID<maxRelCount; ++relID) {
			// Transform relation into BitSet
			BitSet relation = intToRel(relID);
			//System.out.println("relID:\t" + relation.toString());
			// Check whether relation is valid
			if (isValid(relation, queryInfo)) {
				//System.out.println("(is valid)");
				rels.add(relation);
			}
		}
		return rels;
	}
	/**
	 * Groups relations by their cardinality (i.e., number of tables).
	 * 
	 * @param allRels	input relations
	 * @param nrTables	number of tables in query
	 * @return			a mapping from relation cardinality to relations
	 */
	static Map<Integer, Set<BitSet>> groupRels(
			Set<BitSet> allRels, int nrTables) {
		Map<Integer, Set<BitSet>> relsByCard = 
				new HashMap<Integer, Set<BitSet>>();
		// Create empty list for each cardinality
		for (int k=1; k<=nrTables; ++k) {
			relsByCard.put(k, new HashSet<BitSet>());
		}
		// Group all relations by cardinality
		for (BitSet relation : allRels) {
			int card = relation.cardinality();
			relsByCard.get(card).add(relation);
		}
		return relsByCard;
	}
	/**
	 * Maps each relation to a list of valid decompositions.
	 * 
	 * @param allRels		relations to map to decompositions
	 * @param relToSubsets	maps table sets to table subsets
	 * @param relToSupsets	maps table sets to table supersets
	 * @return				mapping from relation to valid table subsets
	 */
	static void decomposeRels(Set<BitSet> allRels,
			Map<BitSet, Set<BitSet>> relToSubsets,
			Map<BitSet, Set<BitSet>> relToSupsets) {
		// Initialize mapping from relations to sub-relations
		for (BitSet rel : allRels) {
			relToSubsets.put(rel, new HashSet<BitSet>());
			relToSupsets.put(rel, new HashSet<BitSet>());
		}
		// Test pairs of valid relations whether 
		// one is a subset of the other.
		for (BitSet rel : allRels) {
			for (BitSet subRel : allRels) {
				if (RelUtil.isSubset(subRel, rel)) {
					// Test if the complement of the 
					// sub-relation is also valid.
					BitSet complement = new BitSet();
					complement.or(rel);
					complement.andNot(subRel);
					if (allRels.contains(complement)) {
						relToSubsets.get(rel).add(subRel);
						relToSupsets.get(subRel).add(rel);
						relToSupsets.get(complement).add(rel);
					}
				}
			}
		}
	}
	/**
	 * Generates a plan for producing the specified target relation,
	 * using a given cost model that is based on relation generation
	 * cost. The relation to generate must be one of the relations
	 * generated during initialization. The method returns not only
	 * the best plan for the target relation but also the best plans
	 * for all valid table subsets of the target relation.
	 * 
	 * @param targetRel		the relation to generate
	 * @param relCost		associates relations with generation cost
	 * @param ignoreTarget	if we ignore cost of writing target relation
	 * @return				a mapping from relations to optimal plans
	 */
	public Map<BitSet, ProbePlan> plan(BitSet targetRel, 
			Map<BitSet, Double> relCost, boolean ignoreTarget) {
		//System.out.println("Planning with relation cost:");
		//System.out.println(relCost.toString());
		// Get the number of tables in target relation
		int cardinality = targetRel.cardinality();
		// Initialize plans for base tables
		//Map<BitSet, Double> bestCost = new HashMap<BitSet, Double>();
		Map<BitSet, ProbePlan> bestPlan = new HashMap<BitSet, ProbePlan>();
		for (int table=targetRel.nextSetBit(0); table>=0; 
				table=targetRel.nextSetBit(table+1)) {
			BitSet tableSet = new BitSet();
			tableSet.set(table);
			double cost = relCost.get(tableSet);
			LeafPlan leafPlan = new LeafPlan(queryInfo, table, cost);
			bestPlan.put(tableSet, leafPlan);
		}
		// Iterate over join relations
		for (int k=2; k<=cardinality; ++k) {
			for (BitSet rel : relsByCard.get(k)) {
				// Check if this relation is relevant
				if (RelUtil.isSubset(rel, targetRel)) {
					for (BitSet subRel1 : relToSubsets.get(rel)) {
						BitSet subRel2 = new BitSet();
						subRel2.or(rel);
						subRel2.andNot(subRel1);
						if (!subRel1.isEmpty() && !subRel2.isEmpty()) {
							/*
							System.out.println("resultRel: \t" + rel.toString());
							System.out.println("SubRel1: \t" + subRel1.toString());
							System.out.println("SubRel2: \t" + subRel2.toString());
							*/
							double cost1 = bestPlan.get(subRel1).cost;
							double cost2 = bestPlan.get(subRel2).cost;
							double oldCost = bestPlan.containsKey(rel)?
									bestPlan.get(rel).cost:
										Double.POSITIVE_INFINITY;
							// We do not count the cost of writing out final result
							double newCost = cost1 + cost2 + 
									(k == cardinality && ignoreTarget? 
											0:relCost.get(rel));
							if (newCost <= oldCost) {
								ProbePlan leftPlan = bestPlan.get(subRel1);
								ProbePlan rightPlan = bestPlan.get(subRel2);
								CompositePlan newPlan = 
										new CompositePlan(
										leftPlan, rightPlan, newCost);
								bestPlan.put(rel, newPlan);
							}
						}
					}
				}
			}
		} // for k
		// Return best plan for entire relation
		return bestPlan;
	}
}
