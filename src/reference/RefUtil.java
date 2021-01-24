package reference;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;

import SQLtools.QueryInfo;
import optimizers.safe.RelInfo;

/**
 * Contains utility methods to read reference solutions
 * from disc and to compare them to solutions found
 * by various optimizers. Also contains some information
 * about database schema (e.g, key-foreign key constraints).
 * 
 * @author immanueltrummer
 *
 */
public class RefUtil {
	/**
	 * Contains reference solution.
	 */
	public final JSONquery jsonQuery;
	/**
	 * Meta-data associated with the query.
	 */
	public final QueryInfo queryInfo;
	/**
	 * Reference cardinality for each relation.
	 */
	public final Map<BitSet, Double> refCard;
	/**
	 * Captures key-foreign key constraints and maps
	 * each table index on the foreign key side to the 
	 * corresponding table index on the key side.
	 */
	public final Map<Integer, Integer> foreignToKey;
	/**
	 * Read data for specified test case from disc.
	 * 
	 * @param experimentsDir		root directory for test cases
	 * 							and experimental results
	 * @param testCaseNr			number of JOB test case
	 * @param queryInfo			meta-data describing the query
	 * @throws Exception
	 */
	public RefUtil(String experimentsDir, int testCaseNr, 
			QueryInfo queryInfo) throws Exception {
		// Generate path to specified solution file
		String filename = experimentsDir + 
				"input/job/reference/q" + testCaseNr;
		// Read data from JSON file on disc
		byte[] jsonData = Files.readAllBytes(Paths.get(filename));
		ObjectMapper objectMapper = new ObjectMapper();
		jsonQuery = objectMapper.readValue(jsonData, JSONquery.class);
		this.queryInfo = queryInfo;
		this.refCard = extractCard(jsonQuery, queryInfo);
		this.foreignToKey = extractConstraints(jsonQuery, queryInfo);
	}
	/**
	 * Extracts reference cardinality for each relation.
	 * 
	 * @param jsonQuery		JSON query object
	 * @param queryInfo		meta-data about query
	 * @return				a mapping from relations to cardinalities
	 */
	static Map<BitSet, Double> extractCard(JSONquery jsonQuery, 
			QueryInfo queryInfo) throws Exception {
		Map<BitSet, Double> refCard = new HashMap<BitSet, Double>();
		// Treat singleton relations
		for (JSONrelation jsonRel : jsonQuery.getrelations()) {
			String alias = jsonRel.getName();
			double curRelCard = jsonRel.getCardinality();
			Integer tableID = queryInfo.tableAliasToID.get(alias);
			if (tableID == null) {
				throw new Exception("Unknown table alias " + alias);
			}
			BitSet rel = new BitSet();
			rel.set(tableID);
			refCard.put(rel, curRelCard);
		}
		// Treat composite relations
		for (JSONsize size : jsonQuery.getsizes()) {
			double curCard = size.getCardinality();
			List<String> aliasList = size.getRelations();
			BitSet rel = new BitSet();
			for (String alias : aliasList) {
				Integer tableID = queryInfo.tableAliasToID.get(alias);
				rel.set(tableID);
			}
			refCard.put(rel, curCard);
		}
		return refCard;
	}
	/**
	 * Extracts key-foreign key relationships between tables.
	 * 
	 * @param jsonQuery		reference data read from disc
	 * @param queryInfo		meta-data on input query
	 * @return				mappings from foreign key to key tables
	 */
	static Map<Integer, Integer> extractConstraints(JSONquery jsonQuery, 
			QueryInfo queryInfo) throws Exception {
		// Iterate over join relationships
		Map<Integer, Integer> foreignToKey = 
				new HashMap<Integer, Integer>();
		for (JSONjoin join : jsonQuery.getJoins()) {
			String keySide = join.getPrimaryKeySide();
			// Check for constraint
			if (keySide != null) {
				// Get alias on foreign key side
				List<String> rels = join.getRelations();
				if (rels.size() != 2) {
					throw new Exception(
							"Unexpected rel count:  " + rels);
				}
				String foreignSide = null;
				for (String rel : rels) {
					if (!rel.equals(keySide)) {
						foreignSide = rel;
					}
				}
				// Get table IDs associated with aliases
				Integer keySideID = queryInfo.tableAliasToID.get(keySide);
				Integer foreignSideID = queryInfo.tableAliasToID.get(foreignSide);
				foreignToKey.put(foreignSideID, keySideID);
			}
		}
		return foreignToKey;
	}
	/**
	 * Returns true iff the first relation is at the foreign
	 * key side of at least one key-foreign key constraint.
	 * 
	 * @param rel1	relation to test for foreign key side
	 * @param rel2	relation to test for key side
	 * @return
	 */
	public boolean onForeignSide(BitSet rel1, BitSet rel2) {
		// Iterate over all key-foreign key constraints
		for (Entry<Integer, Integer> entry : foreignToKey.entrySet()) {
			int foreignSide = entry.getKey();
			int keySide = entry.getValue();
			if (rel1.get(foreignSide) && rel2.get(keySide)) {
				return true;
			}
		}
		return false;
	}
	/**
	 * Analyzes the result of a solver invocation - throws
	 * an exception in case of inconsistencies with the
	 * reference solution. 
	 * 
	 * @param relInfos		maps relations to meta-data
	 * @param timeout		whether the solver had a timeout
	 * @param tolerance		relative tolerance when comparing cardinality
	 * @throws Exception
	 */
	public void testRelInfos(Map<BitSet, RelInfo> relInfos, 
			boolean timeout, double tolerance) throws Exception {
		// Compare number of considered relations
		if (relInfos.size() != refCard.size()) {
			throw new Exception("Inconsistent relation count (" +
					relInfos.size() + " vs. " + refCard.size() + ")");
		}
		System.out.println("Verified relation count");
		// Compare set of considered relations
		for (BitSet refRel : refCard.keySet()) {
			if (!relInfos.containsKey(refRel)) {
				throw new Exception("Reference relation " + 
						refRel.toString() + " was not considered");
			}
		}
		System.out.println("Verified relations");
		// Compare cardinality values
		for (Entry<BitSet, RelInfo> entryToTest : relInfos.entrySet()) {
			BitSet rel = entryToTest.getKey();
			RelInfo info = entryToTest.getValue();
			Double curCard = refCard.get(rel);
			// Verification depends on relation status
			switch (info.relStatus) {
			case PENDING:
			case UNVERIF:
				if (!timeout) {
					throw new Exception("Termination with "
							+ "pending/unverifiable relation");
				}
				break;
			case EXCLUDED:
				if (info.lowerCardBound > curCard * (1+tolerance)) {
					throw new Exception("Cardinality estimate too "
							+ "large for " + rel.toString() + " (" +
							info.lowerCardBound + " vs. " + curCard + 
							")");
				}
				break;
			case VERIFIED:
				if (info.lowerCardBound > curCard * (1+tolerance)) {
					throw new Exception("Cardinality too large for "
							+ "relation " + rel.toString());
				}
				if (info.lowerCardBound < curCard * (1-tolerance)) {
					throw new Exception("Cardinality too small for "
							+ "relation " + rel.toString() +
							" (" + info.lowerCardBound + 
							" instead of " + curCard);
				}
				break;
			default:
				throw new Exception("Unknown relation status for " +
						rel.toString() + " - " + info.toString());
			}
		}
		System.out.println("Verified relation cardinalities");
	}
	/**
	 * Calculates the variance in cardinality of intermediate
	 * results, optionally considering base tables as well.
	 * 
	 * @param ignoreBaseTables	whether to ignore base tables
	 * @return					variance of cardinality values
	 */
	public double cardVariance(boolean ignoreBaseTables) {
		// Extract relevant cardinality values
		List<Double> cardVals = new ArrayList<Double>();
		for (Entry<BitSet, Double> entry : refCard.entrySet()) {
			BitSet rel = entry.getKey();
			if (rel.cardinality() > 1 || !ignoreBaseTables) {
				double card = entry.getValue();
				cardVals.add(card);
			}
		}
		int nrCardVals = cardVals.size();
		// Calculate average for cardinality values
		double cardSum = 0;
		for (double card : cardVals) {
			cardSum += card;
		}
		double cardAvg = cardSum / nrCardVals;
		// Calculate variance
		double squareSum = 0;
		for (double card : cardVals) {
			squareSum += (card - cardAvg) * (card - cardAvg);
		}
		double cardVariance = squareSum / nrCardVals;
		return cardVariance;
	}
}
