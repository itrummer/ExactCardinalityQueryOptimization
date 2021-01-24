package optimizers.safe;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import SQLtools.QueryInfo;
import SQLtools.SQLgenerator;
import common.PlanUtil;
import common.RelUtil;
import common.plans.LeafPlan;
import common.plans.ProbePlan;
import optimizers.Optimizer;
import pgConnector.AnalyzeInfo;
import pgConnector.PgConnector;

/**
 * Simple version of safe approach for generating
 * verified-optimal query plans.
 * 
 * @author immanueltrummer
 *
 */
public class SafeNaive extends Optimizer {
	public SafeNaive(int timeoutMillis, PgConnector pgConnector) {
		super(timeoutMillis, pgConnector);
	}
	/**
	 * Contains information on relations generated during
	 * the last optimizer invocation.
	 */
	public Map<BitSet, RelInfo> relInfos;
	/**
	 * Extracts optimistic cardinality estimates from relation info.
	 * 
	 * @param relInfos			maps each relation to meta-data
	 * @param lower				whether to set lower (instead of upper) bounds
	 * @param ignoreBaseTables	whether to set base cards to zero
	 * @param card				Maps each relation to lower cardinality
	 * 							bound after this invocation.
	 */
	void setCardBounds(Map<BitSet, RelInfo> relInfos, 
			boolean lower, boolean ignoreBaseTables, 
			Map<BitSet, Double> card) {
		// Iterate over relation meta-data
		for (Entry<BitSet, RelInfo> entry : relInfos.entrySet()) {
			BitSet rel = entry.getKey();
			RelInfo info = entry.getValue();
			// Check for base tables
			if (rel.cardinality() == 1 && ignoreBaseTables) {
				card.put(rel, 0.0);
			} else {
				// Whether to calculate lower bounds
				if (lower) {
					// Simply use given lower bound
					card.put(rel, info.lowerCardBound);
				} else {
					// Need to calculate upper bound - the
					// lower bound equals the upper bound
					// only for verified relations, use
					// trivial upper bound otherwise.
					if (info.relStatus == RelStatus.VERIFIED) {
						card.put(rel, info.lowerCardBound);
					} else {
						card.put(rel, Double.POSITIVE_INFINITY);
					}
				} // whether lower or upper bound estimate
			} // base tables
		}
	}
	/**
	 * Analyzes the result of a probe query to determine
	 * whether the exact cardinality of the result relation
	 * can be verified based on that.
	 * 
	 * @param rowCounts	list of actual row counts in extraction order
	 * @param maxCard	cutoff cardinality threshold
	 * @return			true iff the row counts allow verification
	 */
	/*
	boolean isVerified(List<Integer> rowCounts, int maxCard) {
		for (int rowCount : rowCounts) {
			if (rowCount < 0 || rowCount >= maxCard) {
				return false;
			}
		}
		return true;
	}
	*/
	/**
	 * Updates relation meta-data related to cardinality
	 * based on the result of a probing query.
	 * 
	 * @param plan			plan used for cardinality probing
	 * @param maxCard		cardinality threshold during probing
	 * @param analyzeInfo	contains extracted cardinality info
	 * @param relInfos		relation meta-data to update
	 */
	void updateCard(ProbePlan plan, int maxCard, 
			AnalyzeInfo analyzeInfo, Map<BitSet, RelInfo> relInfos) {
		// Update lower cardinality bounds and verification status
		for (Entry<BitSet, Integer> entry : analyzeInfo.relToCard.entrySet()) {
			BitSet rel = entry.getKey();
			Integer card = entry.getValue();
			RelInfo info = relInfos.get(rel);
			info.lowerCardBound = Math.max(
					info.lowerCardBound, card);
			if (analyzeInfo.verifiedRels.contains(rel)) {
				info.relStatus = RelStatus.VERIFIED;
			}
		}
	}
	/**
	 * Updates relation meta-data that refers to execution
	 * cost (i.e., lower execution cost bounds for plans
	 * using specific relations, relation exclusion 
	 * status ...).
	 * 
	 * @param costUB				upper bound on execution cost of best plan
	 * @param ignoreBaseTables	whether to skip updates on base tables
	 * @param planUtil			utility functions for query planning
	 * @param relInfos			maps relations to meta-data
	 */
	void updateCost(double costUB, boolean ignoreBaseTables,
			PlanUtil planUtil, Map<BitSet, RelInfo> relInfos) {
		QueryInfo queryInfo = planUtil.queryInfo;
		int nrTables = queryInfo.nrTables;
		// Calculate lower bounds on generation cost (bottom-up)
		Map<BitSet, Double> cardLBs = new HashMap<BitSet, Double>();
		setCardBounds(relInfos, true, true, cardLBs);
		Map<BitSet, ProbePlan> costLBs = planUtil.plan(
				queryInfo.allTables, cardLBs);
		for (Entry<BitSet, RelInfo> entry : relInfos.entrySet()) {
			BitSet rel = entry.getKey();
			RelInfo info = entry.getValue();
			info.generationCostLB = costLBs.get(rel).cost;
		}
		// Calculate lower bound on completion cost (top-down)
		for (int k=nrTables-1; k>1; --k) {
			// Over relations with current cardinality
			for (BitSet rel : planUtil.relsByCard.get(k)) {
				RelInfo relInfo = relInfos.get(rel);
				relInfo.completionCostLB = Double.POSITIVE_INFINITY;
				// Over valid superset relations
				for (BitSet supRel : planUtil.relToSupsets.get(rel)) {
					BitSet complement = new BitSet();
					complement.or(supRel);
					complement.andNot(rel);
					RelInfo supRelInfo = relInfos.get(supRel);
					RelInfo complRelInfo = relInfos.get(complement);
					// Calculate cost bound on completion cost
					double newCost =  supRelInfo.completionCostLB +
							(supRel.cardinality() != nrTables ?
									cardLBs.get(supRel):0) + 
							complRelInfo.generationCostLB;
					/*
					System.out.println(rel.toString() + 
							" over " + supRel.toString() +
							" cost " + newCost +
							" ");
					*/
					if (newCost <= relInfo.completionCostLB) {
						relInfo.completionCostLB = newCost;
					}
				}
			}
		}
		// Calculate lower bounds on total cost
		for (Entry<BitSet, RelInfo> entry : relInfos.entrySet()) {
			RelInfo info = entry.getValue();
			info.lowerCostBound = info.generationCostLB + 
					info.completionCostLB;
		}
		// Exclude relations based on cost bounds
		for (Entry<BitSet, RelInfo> entry : relInfos.entrySet()) {
			BitSet rel = entry.getKey();
			RelInfo info = entry.getValue();
			// Determine whether to exclude this relation
			boolean baseTable = rel.cardinality() == 1;
			if (info.lowerCostBound > costUB &&
					(!baseTable || !ignoreBaseTables)) {
				if (info.relStatus != RelStatus.EXCLUDED) {
					System.out.println("Excluded " + rel.toString());
					System.out.println("Cost:\t" + info.lowerCostBound);
					System.out.println("CostUB:\t" + costUB);
				}
				info.relStatus = RelStatus.EXCLUDED;
			}
		}
		System.out.println(relInfos.toString());
	}
	/**
	 * Examine relation meta-data and extract all relations
	 * whose status is currently pending (i.e., neither
	 * verified nor definitely excluded). This is typically
	 * called after optimistic bounds on query cost were
	 * updated.
	 * 
	 * @param relInfos			relation meta-data
	 * @param ignoreBaseTables	whether to add base tables
	 * 
	 * @return	a list of relations with status "pending"
	 */
	List<BitSet> getPending(Map<BitSet, RelInfo> relInfos, 
			boolean ignoreBaseTables) {
		List<BitSet> pendingRels = new ArrayList<BitSet>();
		for (Entry<BitSet, RelInfo> entry : relInfos.entrySet()) {
			BitSet rel = entry.getKey();
			RelInfo info = entry.getValue();
			boolean baseTable = rel.cardinality() == 1;
			if (info.relStatus == RelStatus.PENDING &&
					(!baseTable || !ignoreBaseTables)) {
				pendingRels.add(rel);				
			}
		}
		return pendingRels;
	}
	/**
	 * 
	 * @param planUtil
	 * @param relInfos
	 * @return
	 */
	BitSet selectRel(PlanUtil planUtil, List<BitSet> todoRels, 
			Map<BitSet, RelInfo> relInfos) {
		int nrTables = planUtil.queryInfo.nrTables;
		for (int k=2; k<=nrTables; ++k) {
			Set<BitSet> rels = planUtil.relsByCard.get(k);
			for (BitSet rel : rels) {
				if (todoRels.contains(rel) && 
						relInfos.get(rel).relStatus == RelStatus.PENDING) {
					return rel;
				}
			}
		}
		System.out.println("No relations left");
		return null;
	}
	@Override
	public void optimize(QueryInfo queryInfo) throws Exception {
		// Initialize timing variables
		long startMillis = System.currentTimeMillis();
		timeout = false;
		// Initialize utility functions
		PlanUtil planUtil = new PlanUtil(queryInfo);
		System.out.println("All valid relations");
		System.out.println(planUtil.allRels.toString());
		System.out.println("Relations by cardinality");
		System.out.println(planUtil.relsByCard.toString());
		System.out.println(planUtil.relToSubsets.toString());
		System.out.println(planUtil.relToSupsets.toString());
		SQLgenerator sqlGen = new SQLgenerator(queryInfo);
		// Maximal factor by which two plans having the same cost
		// according to the C_out cost metric could differ.
		double costVariance = 1;
		// Maps each relation to optimization-related meta-data
		relInfos = new HashMap<BitSet, RelInfo>();
		for (BitSet rel : planUtil.allRels) {
			relInfos.put(rel, new RelInfo());
		}
		// Special treatment for singleton relations
		int nrTables = queryInfo.nrTables;
		int maxBaseCard = 0;
		for (int table=0; table<nrTables && !timeout; ++table) {
			updateTime(startMillis);
			// Calculate cardinality after applying predicates
			LeafPlan probePlan = new LeafPlan(table, 0);
			int maxCard = Integer.MAX_VALUE;
			String probeSQLstem = sqlGen.safeProbeQuery(
					probePlan, maxCard, true);
			String countSQL = "SELECT COUNT(*) FROM (" + probeSQLstem + ") as temp"; 
			ResultSet result = pgConnector.query(countSQL, timeoutMillis);
			result.next();
			int card = Integer.parseInt(result.getString(1));
			maxBaseCard = Math.max(maxBaseCard, card);
			// Store cardinality and mark table as verified
			BitSet rel = new BitSet();
			rel.set(table);
			RelInfo relInfo = relInfos.get(rel);
			relInfo.lowerCardBound = card;
			relInfo.relStatus = RelStatus.VERIFIED;
		}
		// Relations to treat with current cost bounds
		List<BitSet> todoRels = getPending(relInfos, true);
		// Temporary variable mapping relations to cardinality estimates
		Map<BitSet, Double> cardEstimates = new HashMap<BitSet, Double>();
		// Initialize optimistic query cost bound
		// (i.e., bounds on optimal query plan cost).
		double costLB = 100;
		double costUB = Double.POSITIVE_INFINITY;
		// While relations left to treat and no timeout
		while (!todoRels.isEmpty() && !timeout) {
			// Update timeout flag
			updateTime(startMillis);
			// Pick next relation for cardinality probing
			//BitSet rel = todoRels.iterator().next();
			BitSet rel = selectRel(planUtil, todoRels, relInfos);
			todoRels.remove(rel);
			System.out.println("Rel selected:\t" + rel.toString());
			// Check whether relation is still pending
			if (relInfos.get(rel).relStatus == RelStatus.PENDING) {
				// Calculate best probe join order 
				// (using upper cardinality bounds).
				setCardBounds(relInfos, false, true, cardEstimates);
				ProbePlan probePlan = planUtil.plan(
						rel, cardEstimates).get(rel);
				// Maximal cardinality bound for optimistic query cost
				int maxCard = (int)Math.ceil(costLB * costVariance);
				// Generate query for cardinality probing
				String probeSQLstem = sqlGen.safeProbeQuery(
						probePlan, maxCard, true);
				// Extract cardinality measures from query result
				AnalyzeInfo analyzeInfo = new AnalyzeInfo(pgConnector, 
						queryInfo, probePlan, maxCard, timeoutMillis, 
						probeSQLstem);
				System.out.println(analyzeInfo.relToCard.toString());
				System.out.println(analyzeInfo.verifiedRels.toString());
				// Update cardinality-related relation meta-data
				updateCard(probePlan, maxCard, analyzeInfo, relInfos);
			} // if relation status is pending
			// Update cost-related relation meta-data
			setCardBounds(relInfos, false, true, cardEstimates);
			System.out.println();
			//System.out.println(cardEstimates.toString());
			ProbePlan bestPessimisticPlan = planUtil.plan(
					queryInfo.allTables, cardEstimates).get(
							queryInfo.allTables);
			costUB = bestPessimisticPlan.cost;
			updateCost(costUB, true, planUtil, relInfos);
			System.out.println("Upper query cost bound:\t" + costUB);
			// No relations left to treat?
			if (todoRels.isEmpty()) {
				todoRels.addAll(getPending(relInfos, true));
				System.out.println("No TODO rels left");
				System.out.println(todoRels.toString());
				// Check if lower cost bound was updated.
				//if (costUB > costLB) {
					// We need to increase cost bounds in that case
					costLB *= 10;
					System.out.println("Updated cost bounds to " + costLB);
				//}
			}
		} // until optimization finished
		// Output cardinality of all relations
		System.out.println("Info on all relations by cardinality:");
		for (int k=1; k<=nrTables; ++k) {
			Set<BitSet> rels = planUtil.relsByCard.get(k);
			for (BitSet rel : rels) {
				RelInfo info = relInfos.get(rel);
				Set<String> aliasSet = RelUtil.aliasSet(
						rel, queryInfo.tableIDtoAlias);
				System.out.println(aliasSet + ":");
				System.out.println(info.toString());
			}
		}
		updateTime(startMillis);
		System.out.println("Timeout:\t" + timeout);
		System.out.println("Millis:\t" + totalMillis);
	}
}