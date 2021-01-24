package optimizers.safe;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
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
import pgConnector.CardStatus;
import pgConnector.ExplainInfo;
import pgConnector.PgConnector;
import reference.RefUtil;

/**
 * Simple version of safe approach for generating
 * verified-optimal query plans.
 * 
 * @author immanueltrummer
 *
 */
public class SafeNaive2 extends Optimizer {
	public SafeNaive2(int timeoutMillis, PgConnector pgConnector) {
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
	 * @param costModel			how to assign rels to cardinality
	 * @param ignoreBaseTables	whether to set base cards to zero
	 * @param card				Maps each relation to lower cardinality
	 * @param limit				current cardinality limit
	 * 							bound after this invocation.
	 */
	void extractCard(Map<BitSet, RelInfo> relInfos, 
			CostModel costModel, boolean ignoreBaseTables, 
			Map<BitSet, Double> card, int limit) throws Exception {
		// Get highest known cardinality
		double maxCard = 0;
		for (RelInfo info : relInfos.values()) {
			maxCard = Math.max(maxCard, info.lowerCardBound);
		}
		// Iterate over relation meta-data
		for (Entry<BitSet, RelInfo> entry : relInfos.entrySet()) {
			BitSet rel = entry.getKey();
			RelInfo info = entry.getValue();
			// Check for base tables
			if (rel.cardinality() == 1 && ignoreBaseTables) {
				card.put(rel, 0.0);
			} else {
				switch (costModel) {
				case LOWER_BOUNDS:
					card.put(rel, info.lowerCardBound);
					break;
				case UPPER_BOUNDS:
					if (info.relStatus == RelStatus.VERIFIED) {
						card.put(rel, info.lowerCardBound);
					} else {
						card.put(rel, Double.POSITIVE_INFINITY);
					}
					break;
				case BEST_GUESS:
					if (info.relStatus == RelStatus.VERIFIED ||
							info.relStatus == RelStatus.PENDING) {
						card.put(rel, info.cardBestGuess);
					} else {
						card.put(rel, Double.POSITIVE_INFINITY);
					}
					break;
				case SAFE_GUESS:
					if (info.cardBestGuess >= limit) {
						card.put(rel, Double.POSITIVE_INFINITY);
					} else {
						card.put(rel, info.cardBestGuess);
					}
					break;
				case VERIFIABILITY:
					if (info.relStatus == RelStatus.VERIFIED) {
						card.put(rel, info.lowerCardBound);
					} else if (info.relStatus == RelStatus.PENDING) {
						card.put(rel, maxCard);
					} else {
						card.put(rel, Double.POSITIVE_INFINITY);
					}
					break;
				case NR_VERIFIABLE:
					if (rel.cardinality() == 1 && ignoreBaseTables) {
						card.put(rel, 0.0);
					} else if (info.lowerCardBound >= limit) {
						card.put(rel, Double.POSITIVE_INFINITY);
					} else if (info.relStatus != RelStatus.PENDING) {
						card.put(rel, 1.0);
					} else {
						card.put(rel, -2.0);
					}
					/*
					if (info.relStatus == RelStatus.PENDING) {
						card.put(rel, -1.0);
					} else if (info.relStatus == RelStatus.VERIFIED) {
						card.put(rel, 0.0);
					} else {
						card.put(rel, Double.POSITIVE_INFINITY);
					}
					*/
					break;
				default:
					throw new Exception("Unsupported cost model");
				}
			} // base tables
		}
	}
	/**
	 * Updates bounds on relation cardinality
	 * based on the result of a probing query.
	 * 
	 * @param planUtil		auxiliary methods for query planning
	 * @param plan			plan used for cardinality probing
	 * @param cardBudget		cardinality threshold during probing
	 * @param analyzeInfo	contains extracted cardinality info
	 * @param relInfos		relation meta-data to update
	 * @param refUtil		information on key-foreign key constraints
	 */
	void updateCard(PlanUtil planUtil, ProbePlan plan, int cardBudget, 
			AnalyzeInfo analyzeInfo, Map<BitSet, RelInfo> relInfos,
			RefUtil refUtil) throws Exception {
		// Update lower cardinality bounds and verification status
		for (Entry<BitSet, Integer> entry : 
			analyzeInfo.relToCard.entrySet()) {
			BitSet rel = entry.getKey();
			Integer card = entry.getValue();
			RelInfo info = relInfos.get(rel);
			info.lowerCardBound = Math.max(
					info.lowerCardBound, card);
			info.cardBestGuess = Math.max(
					info.cardBestGuess, card);
			// Update upper bound depending on status
			if (analyzeInfo.relToCardStatus.get(rel) == 
					CardStatus.EXACT) {
				info.upperCardBound = card;
			}
			if (info.lowerCardBound > info.upperCardBound) {
				throw new Exception("Lower cardinality bound " +
						"above upper bound for relation " +
						rel.toString() + " " + info.toString());
			}
		}
		// Propagate cardinality bounds top-down
		QueryInfo queryInfo = analyzeInfo.queryInfo;
		int nrTables = queryInfo.nrTables;
		for (int k=nrTables; k>=2; --k) {
			for (BitSet rel : planUtil.relsByCard.get(k)) {
				RelInfo info = relInfos.get(rel);
				for (BitSet subRel : planUtil.relToSubsets.get(rel)) {
					BitSet subRel2 = new BitSet();
					subRel2.or(rel);
					subRel2.andNot(subRel);
					if (refUtil.onForeignSide(subRel, subRel2)) {
						/*
						System.out.println("Updated " + 
								subRel.toString() + 
								" using " + rel.toString());
						*/
						/*
						RelInfo infoS1 = relInfos.get(subRel);
						infoS1.lowerCardBound = Math.max(
								infoS1.lowerCardBound, 
								info.lowerCardBound);
						infoS1.cardBestGuess = Math.max(
								infoS1.cardBestGuess,
								info.lowerCardBound);
						*/
						// TODO: careful, does this propagate errors?
						/*
						infoS1.upperCardBound = Math.min(
								infoS1.upperCardBound,
								info.upperCardBound);
						*/
					}
				}
			}
		}
	}
	/**
	 * Updates relation meta-data that refers to execution
	 * cost (i.e., lower execution cost bounds for plans
	 * using specific relations).
	 * 
	 * @param ignoreBaseTables	whether to skip updates on base tables
	 * @param planUtil			utility functions for query planning
	 * @param relInfos			maps relations to meta-data
	 */
	void updateCost(boolean ignoreBaseTables, PlanUtil planUtil, 
			Map<BitSet, RelInfo> relInfos) throws Exception {
		QueryInfo queryInfo = planUtil.queryInfo;
		int nrTables = queryInfo.nrTables;
		// Calculate lower bounds on generation cost (bottom-up)
		Map<BitSet, Double> cardLBs = new HashMap<BitSet, Double>();
		extractCard(relInfos, CostModel.LOWER_BOUNDS, true, cardLBs, -1);
		Map<BitSet, ProbePlan> costLBs = planUtil.plan(
				queryInfo.allTables, cardLBs, true);
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
	}
	/**
	 * Returns true iff there is at least one decomposition
	 * of a given relation into two join operands, such that
	 * both operand relations are assigned to one out of a
	 * list of admissible relation status values.
	 * 
	 * @param rel			a relation to decompose
	 * @param planUtil		used for the decomposition
	 * @param validStatus	allowed status for join operands
	 * @return				true iff valid decomposition exists
	 */
	boolean validDecomposition(BitSet rel, PlanUtil planUtil, 
			Set<RelStatus> validStatus) {
		for (BitSet subRel : planUtil.relToSubsets.get(rel)) {
			BitSet subRel2 = new BitSet();
			subRel2.or(rel);
			subRel2.andNot(subRel);
			RelInfo subRelInfo = relInfos.get(subRel);
			RelInfo subRelInfo2 = relInfos.get(subRel2);
			if (validStatus.contains(subRelInfo.relStatus) &&
					validStatus.contains(subRelInfo2.relStatus)) {
				return true;
			}
		}
		return false;
	}
	/**
	 * Calculates upper bound on the cost of an optimal plan.
	 * 
	 * @param queryInfo		meta-data about input query
	 * @param planUtil		planning utility methods
	 * @return				upper bound cost estimate
	 * @throws Exception
	 */
	public double bestCostUB(QueryInfo queryInfo, PlanUtil planUtil) 
			throws Exception {
		Map<BitSet, Double> cardEstimates = new HashMap<BitSet, Double>();
		extractCard(relInfos, CostModel.UPPER_BOUNDS, 
				true, cardEstimates, -1);
		ProbePlan bestPessimisticPlan = planUtil.plan(
				queryInfo.allTables, cardEstimates, true).get(
						queryInfo.allTables);
		return bestPessimisticPlan.cost;
	}
	/**
	 * Updates the status of all relations.
	 * 
	 * @param queryInfo			meta-data about query
	 * @param planUtil			utility methods for planning
	 * @param ignoreBaseTables	whether to ignore base tables
	 * @param limit				cardinality limit per result relation
	 * @param limitUpdated		if cardinality limit was just updated
	 */
	public void updateStatus(QueryInfo queryInfo, 
			PlanUtil planUtil, boolean ignoreBaseTables,
			int limit, boolean limitUpdated) throws Exception {
		// Calculate upper bound for optimal plan cost
		double bestCostUB = bestCostUB(queryInfo, planUtil);
		double bestCostLB = relInfos.get(queryInfo.allTables).generationCostLB;
		System.out.println("Upper bound on best cost:\t" + bestCostUB);
		System.out.println("Lower bound on best cost:\t" + bestCostLB);
		System.out.println("Limit:\t" + limit);
		// Mark pending relations as verified if possible
		int kStart = ignoreBaseTables ? 2:1;
		for (int k=kStart; k<=queryInfo.nrTables; ++k) {
			Set<BitSet> rels = planUtil.relsByCard.get(k);
			for (BitSet rel : rels) {
				RelInfo info = relInfos.get(rel);
				// Re-insert temporarily excluded relations
				// in case of a cardinality limit increase.
				if (info.relStatus == RelStatus.UNVERIF && 
						limitUpdated) {
					info.relStatus = RelStatus.PENDING;
				}
				// Try verifying relation
				if (info.relStatus == RelStatus.PENDING) {
					if (info.lowerCardBound * 1.01 >= 
							info.upperCardBound) {
						System.out.println("Verified " + rel.toString());
						info.relStatus = RelStatus.VERIFIED;
					}
				}
				// Try permanently excluding relation directly
				{
					Set<RelStatus> validStates = new HashSet<RelStatus>();
					validStates.add(RelStatus.VERIFIED);
					validStates.add(RelStatus.PENDING);
					validStates.add(RelStatus.UNVERIF);
					if (info.lowerCostBound > bestCostUB ||
							!validDecomposition(rel, 
									planUtil, validStates)) {
						if (info.relStatus != RelStatus.EXCLUDED) {
							System.out.println("Excluded " + 
									rel.toString() + " " + 
									info.toString());							
						}
						info.relStatus = RelStatus.EXCLUDED;
					}					
				}
				// Try excluding pending relations temporarily
				if (info.relStatus == RelStatus.PENDING) {
					Set<RelStatus> validStates = new HashSet<RelStatus>();
					validStates.add(RelStatus.VERIFIED);
					validStates.add(RelStatus.PENDING);
					if (info.lowerCardBound >= limit ||
							!validDecomposition(rel, 
									planUtil, validStates)) {
						System.out.println("Temporarily excluded " + rel.toString());
						info.relStatus = RelStatus.UNVERIF;						
					}
				}
			} // over all k-relations
		} // over relation cardinality
	}
	/**
	 * Examine relation meta-data and extract all relations
	 * with a specified relation status.
	 * 
	 * @param relInfos			relation meta-data
	 * @param ignoreBaseTables	whether to add base tables
	 * @param relStatus			get relations of that status
	 * 
	 * @return	a list of relations with given status
	 */
	List<BitSet> getByStatus(Map<BitSet, RelInfo> relInfos, 
			boolean ignoreBaseTables, RelStatus relStatus) {
		List<BitSet> resultRels = new ArrayList<BitSet>();
		for (Entry<BitSet, RelInfo> entry : relInfos.entrySet()) {
			BitSet rel = entry.getKey();
			RelInfo info = entry.getValue();
			boolean baseTable = rel.cardinality() == 1;
			if (info.relStatus == relStatus &&
					(!baseTable || !ignoreBaseTables)) {
				resultRels.add(rel);				
			}
		}
		return resultRels;
	}
	/**
	 * Selects next relation to verify.
	 * 
	 * @param planUtil	contains admissible relations
	 * @param relInfos	meta-data about relation status
	 * @return			next relation to verify
	 */
	BitSet selectRel(PlanUtil planUtil, Map<BitSet, RelInfo> relInfos) {
		int nrTables = planUtil.queryInfo.nrTables;
		for (int k=2; k<=nrTables; ++k) {
		//for (int k=nrTables; k>=2; --k) {
			Set<BitSet> rels = planUtil.relsByCard.get(k);
			for (BitSet rel : rels) {
				if (relInfos.get(rel).relStatus == RelStatus.PENDING) {
					return rel;
				}
			}
		}
		System.out.println("No relations left");
		return null;
	}
	/**
	 * Verifies cardinality of base relations (after
	 * applying all relevant predicates) by executing
	 * simple count queries. Returns the cardinality
	 * of the largest base relation after filtering.
	 * 
	 * @param queryInfo		query meta-data
	 * @param sqlGen			auxiliary methods for generating queries
	 * @return				maximal base table cardinality after filter
	 * @throws Exception
	 */
	public int verifyBaseTables(QueryInfo queryInfo, 
			SQLgenerator sqlGen) throws Exception {
		// Special treatment for singleton relations
		int nrTables = queryInfo.nrTables;
		int maxBaseCard = 0;
		for (int table=0; table<nrTables && !timeout; ++table) {
			// Calculate cardinality after applying predicates
			LeafPlan probePlan = new LeafPlan(queryInfo, table, 0);
			int maxCard = Integer.MAX_VALUE;
			String probeSQLstem = sqlGen.safeProbeQuery(
					queryInfo, probePlan, maxCard, true);
			String countSQL = "SELECT COUNT(*) FROM (" + 
					probeSQLstem + ") as temp"; 
			System.out.println(countSQL);
			ResultSet result = pgConnector.query(countSQL, timeoutMillis);
			result.next();
			int card = Integer.parseInt(result.getString(1));
			maxBaseCard = Math.max(maxBaseCard, card);
			// Store cardinality and mark table as verified
			BitSet rel = new BitSet();
			rel.set(table);
			RelInfo relInfo = relInfos.get(rel);
			relInfo.lowerCardBound = card;
			relInfo.cardBestGuess = card;
			relInfo.relStatus = RelStatus.VERIFIED;
		}
		return maxBaseCard;
	}
	/**
	 * Select plan used to obtain cardinality estimates.
	 * 
	 * @param queryInfo		meta-data about input query
	 * @param planUtil		auxiliary planning methods
	 * @param limit			cardinality limit
	 * @return				a cardinality probing plan
	 * @throws Exception
	 */
	ProbePlan pickProbePlan(QueryInfo queryInfo, 
			PlanUtil planUtil, int limit) throws Exception {
		// Calculate upper bound on optimal cost
		double bestCostUB = bestCostUB(queryInfo, planUtil);
		boolean completePlan = bestCostUB != Double.POSITIVE_INFINITY;
		Map<BitSet, Double> cardVals = new HashMap<BitSet, Double>();
		BitSet allTables = queryInfo.allTables;
		ProbePlan probePlan = null;
		// If we have no complete plan yet
		if (!completePlan) {
			System.out.println("Trying to find a complete plan");
			extractCard(relInfos, CostModel.SAFE_GUESS, 
					true, cardVals, limit);
			probePlan = planUtil.plan(allTables, 
					cardVals, true).get(allTables);
		}
		// If no interesting plan found
		if (probePlan == null || probePlan.cost == Double.POSITIVE_INFINITY) {
			probePlan = null;
			System.out.println("Trying to verify maximal number of rels");
			extractCard(relInfos, CostModel.NR_VERIFIABLE, 
					true, cardVals, limit);
			int nrTables = queryInfo.nrTables;
			double bestCost = Double.POSITIVE_INFINITY;
			for (int k=2; k<=nrTables; ++k) {
				for (BitSet rel : planUtil.relsByCard.get(k)) {
					if (relInfos.get(rel).relStatus == RelStatus.PENDING) {
						// Check whether it is verifiable
						ProbePlan plan = planUtil.plan(
								rel, cardVals, false).get(rel);
						//System.out.println("Rel" + rel.toString());
						if (plan.cost < bestCost) {
							probePlan = plan;
							bestCost = plan.cost;
						}
					}
				}
			}
			
			/*
			Map<BitSet, ProbePlan> bestPlans = new HashMap<BitSet, ProbePlan>();
			int nrTables = queryInfo.nrTables;
			for (int table=0; table<nrTables; ++table) {
				BitSet rel = new BitSet();
				rel.set(table);
				LeafPlan plan = new LeafPlan(queryInfo, table, 0);
				bestPlans.put(rel, plan);
			}
			for (int k=2; k<=nrTables; ++k) {
				for (BitSet rel : planUtil.relsByCard.get(k)) {
					for (BitSet subRel : planUtil.relToSubsets.get(rel)) {
						BitSet subRel2 = new BitSet();
						subRel2.or(rel);
						subRel2.andNot(subRel);
						
					}
				}
			}
			*/
			/*
			extractCard(relInfos, CostModel.NR_VERIFIABLE, 
					true, cardVals, -1);
			Map<BitSet, ProbePlan> bestPlans = planUtil.plan(
					allTables, cardVals, false);
			double bestCost = Double.POSITIVE_INFINITY;
			ProbePlan bestPlan = null;
			for (int k=2; k<=queryInfo.nrTables; ++k) {
				for (BitSet rel : planUtil.relsByCard.get(k)) {
					ProbePlan relPlan = bestPlans.get(rel);
					//int nrVerified = k - 1 - (int)relPlan.cost;
					if (relPlan.cost < bestCost) {
						bestCost = relPlan.cost;
						bestPlan = relPlan;
					}
				}
			}
			probePlan = bestPlan;
			System.out.println("maxNrVerified:\t" + bestCost);
			System.out.println("Probe result:\t" + bestPlan.resultRel.toString());
			if (bestCost >= 0.0) {
				System.out.println("Treating arbitrary pending rel");
				BitSet rel = getPending(relInfos, true).iterator().next();
				extractCard(relInfos, CostModel.SAFE_GUESS, 
						true, cardVals, limit);
				probePlan = planUtil.plan(rel, cardVals, true).get(rel);
			}
			*/
		}
		return probePlan;
		/*
		Map<BitSet, Double> cardVals = new HashMap<BitSet, Double>();
		extractCard(relInfos, CostModel.UPPER_BOUNDS, true, cardVals, -1);
		double minCost = Double.POSITIVE_INFINITY;
		ProbePlan bestPlan = null;
		int nrTables = queryInfo.nrTables;
		for (int k=2; k<=nrTables; ++k) {
			for (BitSet rel : planUtil.relsByCard.get(k)) {
				if (relInfos.get(rel).relStatus == RelStatus.PENDING) {
					ProbePlan plan = planUtil.plan(rel, cardVals).get(rel);
					if (plan.cost < minCost) {
						bestPlan = plan;
						minCost = plan.cost;
					}					
				}
			}
		}
		if (bestPlan == null) {
			throw new Exception("No probe plan found");
		}
		return bestPlan;
		*/
	}
	@Override
	public void optimize(QueryInfo queryInfo, RefUtil refUtil) 
			throws Exception {
		System.out.println("Started optimization");
		// Initialize timing variables
		long startMillis = System.currentTimeMillis();
		timeout = false;
		nrQueries = 0;
		// Initialize utility functions
		PlanUtil planUtil = new PlanUtil(queryInfo);
		SQLgenerator sqlGen = new SQLgenerator(queryInfo);
		// Generate debugging output
		//System.out.println("All valid relations");
		//System.out.println(planUtil.allRels.toString());
		//System.out.println("Relations by cardinality");
		//System.out.println(planUtil.relsByCard.toString());
		//System.out.println(planUtil.relToSubsets.toString());
		//System.out.println(planUtil.relToSupsets.toString());
		// Maps each relation to optimization-related meta-data
		relInfos = new HashMap<BitSet, RelInfo>();
		for (BitSet rel : planUtil.allRels) {
			// Initialize best guess from optimizer
			RelInfo info = new RelInfo();
			String countQuery = sqlGen.countQuery(rel);
			ExplainInfo explain = new ExplainInfo(countQuery, 
					queryInfo.tableAliasToID, pgConnector);
			info.cardBestGuess = explain.card;
			relInfos.put(rel, info);
		}
		// Determine cardinality of all base tables
		int maxBaseCard = verifyBaseTables(queryInfo, sqlGen);
		System.out.println("All base tables verified");
		// Initialize cardinality budget (conservatively)
		//int limit = 100;
		//int limit = maxBaseCard / 10000;
		int limit = maxBaseCard / 50;
		//int limit = 100000;
		// While relations left to treat and no timeout
		while (!getByStatus(relInfos, true, 
				RelStatus.PENDING).isEmpty() && !timeout) {
			// Update timeout flag
			updateTime(startMillis);
			// Select plan for cardinality probing
			ProbePlan probePlan = pickProbePlan(
					queryInfo, planUtil, limit);
			// Have plan which may make progress?
			if (probePlan != null) {
				// Execute probe plan to get cardinality values
				System.out.println("Probe plan result:\t" + probePlan.resultRel.toString());
				String probeSQLstem = sqlGen.safeProbeQuery(
						queryInfo, probePlan, limit, true);
				System.out.println(probeSQLstem);
				AnalyzeInfo analyzeInfo = new AnalyzeInfo(pgConnector, 
						queryInfo, probePlan, limit, timeoutMillis, 
						probeSQLstem);
				++nrQueries;
				System.out.println("Cardinality limit:\t" + limit);
				System.out.println(analyzeInfo.relToCardStatus.toString());
				// Update cardinality and cost bounds based on probe
				updateCard(planUtil, probePlan, limit, 
						analyzeInfo, relInfos, refUtil);
				updateCost(true, planUtil, relInfos);
				// Update relation status
				updateStatus(queryInfo, planUtil, true, limit, false);
				//System.out.println(relInfos.toString());
				int nrPending = getByStatus(relInfos, true, 
						RelStatus.PENDING).size();
				int nrUnverif = getByStatus(relInfos, true,
						RelStatus.UNVERIF).size();
				System.out.println("Nr. pending:\t" + nrPending);
				System.out.println("Nr. unverif:\t" + nrUnverif);
				System.out.println("Total millis:\t" + totalMillis);
				/*
				System.out.println(getPending(relInfos, true).toString());
				for (BitSet pendingRel : getPending(relInfos, true)) {
					System.out.println("Decompositions for " + pendingRel.toString());
					System.out.println(
							planUtil.relToSubsets.get(pendingRel).toString());
				}
				*/
				/*
				// Decrease limit considering optimal plan cost
				double bestCostUB = bestCostUB(queryInfo, planUtil);
				limit = Math.min(limit, (int)Math.ceil(bestCostUB));
				*/				
			}
			if (probePlan == null || getByStatus(relInfos, 
					true, RelStatus.PENDING).isEmpty()) {
			// Do we need to increase cardinality limit?
			//if (getPending(relInfos, true).isEmpty()) {
				limit *= 10;
				System.out.println("Limit update to " + limit);
				// See whether new relations are activated
				updateStatus(queryInfo, planUtil, true, limit, true);
			}
		} // until optimization finished
		// Output cardinality of all relations
		System.out.println("Info on all relations by cardinality:");
		int nrTables = queryInfo.nrTables;
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