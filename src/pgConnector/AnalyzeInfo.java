package pgConnector;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import SQLtools.QueryInfo;
import common.plans.CompositePlan;
import common.plans.ProbePlan;

/**
 * Extracts information from the result of an
 * explain-analyze query.
 * 
 * @author immanueltrummer
 *
 */
public class AnalyzeInfo {
	/**
	 * Connection to the database.
	 */
	public final PgConnector pgConnector;
	/**
	 * Contains information about an input query.
	 */
	public final QueryInfo queryInfo;
	/**
	 * Maps intermediate result relations to a cardinality
	 * that was extracted from the analysis query result -
	 * this represents typically a lower bound on the
	 * cardinality since queries may include limit clauses.
	 * A cardinality of -1 indicates a plan branch
	 * that was never executed.
	 */
	public final Map<BitSet, Integer> relToCard;
	/**
	 * Maps each relation to cardinality status, indicating
	 * whether the extracted value is exact or a lower bound.
	 */
	public final Map<BitSet, CardStatus> relToCardStatus;
	/**
	 * Executes explain-analyze query on the database
	 * and extract properties from query result.
	 * 
	 * @param pgConnector	connection to the database
	 * @param queryInfo		information about the input query
	 * @param probePlan		probing plan behind sub-query
	 * @param cardLimit		cardinality limit for each result
	 * @param timeoutMillis	number of milliseconds until timeout
	 * @param subQuery		a sub-query to analyze
	 */
	public AnalyzeInfo(PgConnector pgConnector, QueryInfo queryInfo, 
			ProbePlan probePlan, int cardLimit, int timeoutMillis, 
			String subQuery) throws Exception {
		this.pgConnector = pgConnector;
		this.queryInfo = queryInfo;
		// Issue explain-analyze query to database
		String analyzeQuery = "EXPLAIN ANALYZE " + subQuery;
		ResultSet result = pgConnector.query(analyzeQuery, 
				timeoutMillis);
		List<String> resultLines = new ArrayList<String>();
		while (result.next()) {
			resultLines.add(result.getString(1));
		}
		// Extract intermediate result cardinalities
		relToCard = new HashMap<BitSet, Integer>();
		int nrLines = resultLines.size();
		// (we do not consider the last two lines showing
		// planning time and execution time).
		extractInfoRec(resultLines, 0, nrLines-3);
		System.out.println("After extractions:\t" + relToCard.toString());
		// Verify consistency with different extraction method
		List<Integer> rowCounts = extractRowCounts(resultLines, true);
		Set<Integer> rowCountSet = new HashSet<Integer>();
		Set<Integer> rowCountSet2 = new HashSet<Integer>();
		rowCountSet.addAll(rowCounts);
		rowCountSet2.addAll(relToCard.values());
		if (!rowCountSet.equals(rowCountSet2)) {
			System.out.println("Extracted by old method:\t" + rowCountSet.toString());
			System.out.println("Extracted by new method:\t" + rowCountSet2.toString());
			throw new Exception("Inconsistent extractions");
		}
		// Verify consistency with probing plan
		for (BitSet rel : relToCard.keySet()) {
			if (!probePlan.findRel(rel)) {
				throw new Exception("Extracted cardinality for relation " +
						rel.toString() + " but not found in probe plan");
			}
		}
		// Identify guaranteed final cardinality numbers
		this.relToCardStatus = new HashMap<BitSet, CardStatus>();
		verifyRec(probePlan, cardLimit);
	}
	/**
	 * Extracts a list of actual row counts measured for
	 * intermediate results during the execution of a
	 * query (via Postgres' explain analyze command).
	 * 
	 * @param resultLines		lines of analyze query result in order
	 * @param ignoreBaseTables	whether not to extract counts for scans
	 * 							(index or sequential) on base tables.
	 * @return					a list of actual row counts -
	 * 							row count -1 indicates that the
	 * 							corresponding result was never
	 * 							generated - the first row count
	 * 							is refers to the query result. 
	 */
	public List<Integer> extractRowCounts(List<String> resultLines, 
			boolean ignoreBaseTables) throws Exception {
		// Filter out lines that refer to base tables
		StringBuilder filterBuilder = new StringBuilder();
		for (String line : resultLines) {
			if (!ignoreBaseTables || line.contains("->  Limit") ||
					line.startsWith("Limit")) {
				//System.out.println("Recognized limit");
				filterBuilder.append(line);
			}
		}
		String filteredAnalyzeResult = filterBuilder.toString();
		// Prepare extraction patterns
		Pattern actualMeasurePattern = Pattern.compile(
				"\\(actual\\stime=[0-9|\\.]*\\srows=\\d+\\s[^\\s]*\\)|" +
						"\\(never\\sexecuted\\)");
		Pattern rowCountPattern = Pattern.compile(
				"rows=\\d+[\\s|\\)]");
		// Extract actual row counts
		Matcher actualMeasureMatcher = 
				actualMeasurePattern.matcher(filteredAnalyzeResult);
		List<Integer> actualRowCounts = new ArrayList<Integer>();
		while (actualMeasureMatcher.find()) {
			String actualMeasure = actualMeasureMatcher.group();
			if (actualMeasure.equals("(never executed)")) {
				actualRowCounts.add(-1);
			} else {
				Matcher rowCountMatcher = 
						rowCountPattern.matcher(actualMeasure);
				if (!rowCountMatcher.find()) {
					throw new Exception("Error matching row count!");
				}
				String rowCount = rowCountMatcher.group();
				String count = rowCount.substring(5).trim();
				actualRowCounts.add(Integer.parseInt(count));
			}
		}
		return actualRowCounts;
	}
	/**
	 * Returns the number of leading white spaces
	 * for a given string.
	 * 
	 * @param line	string to search for white spaces
	 * @return		count of leading white spaces
	 */
	int countLeadingSpaces(String line) {
		int spaceCtr = 0;
		while (spaceCtr < line.length() && line.charAt(spaceCtr) == ' ') {
			++spaceCtr;
		}
		return spaceCtr;
	}
	/**
	 * Extracts row counts and associated intermediate result
	 * relations from parts of an explain-analyze query result.
	 * 
	 * @param resultLines	lines of explain-analyze query result
	 * @param start			index of first line to consider
	 * @param end			index of last line to consider
	 * @return				relations scanned in current scope
	 * @throws Exception
	 */
	BitSet extractInfoRec(List<String> resultLines, int start, int end) 
			throws Exception {
		//System.out.println("Start:\t" + start + "\tEnd:\t" + end);
		// Check for end of recursion
		if (start > end) {
			return new BitSet();
		}
		// Check whether start line describes new plan node
		String startLine = resultLines.get(start);
		int startIndent = countLeadingSpaces(startLine);
		boolean newNode = startLine.matches("\\s*->.*") || 
				startIndent == 0;
		// Find out what relation this node generates
		BitSet thisRel = new BitSet();
		if (newNode) {
			// Perform one or two recursive invocations
			String siblingIndicator = String.format(
					"%1$"+(startIndent+2)+ "s", "->");
			int nrSiblings = 0;
			int siblingPos = -1;
			for (int lineCtr=start+1; lineCtr<=end; ++lineCtr) {
				String line = resultLines.get(lineCtr);
				if (line.startsWith(siblingIndicator)) {
					nrSiblings++;
					siblingPos = lineCtr;
				}
			}
			if (nrSiblings > 1) {
				throw new Exception("Too many siblings");
			}
			if (nrSiblings == 1) {
				thisRel.or(extractInfoRec(resultLines, start, siblingPos-1));
				thisRel.or(extractInfoRec(resultLines, siblingPos, end));
			} else {
				// Check for table scans in start line
				for (Entry<String, String> entry : 
					queryInfo.tableAliasToName.entrySet()) {
					String alias = entry.getKey();
					String name = entry.getValue();
					String scanIndicator = "on " + name + " " + alias;
					if (startLine.contains(scanIndicator)) {
						Integer tableID = queryInfo.tableAliasToID.get(alias);
						thisRel.set(tableID);
						//System.out.println("Recognized scan on " + alias);
						break;
					}
				}				
				thisRel.or(extractInfoRec(resultLines, start+1, end));
			}
			// Check whether it is a limit node
			if (startLine.contains("Limit ")) {
				//System.out.println("Recognized limit node");
				// Are no actual numbers available due to skipping?
				if (startLine.contains("(never executed)")) {
					relToCard.put(thisRel, -1);
				} else {
					// Extract string describing actual measures
					Pattern actualPattern = Pattern.compile(
							"\\(actual\\stime=[0-9|\\.]*\\s" + 
									"rows=\\d+\\s[^\\s]*\\)");
					Matcher actualMatcher = actualPattern.matcher(
							startLine);
					if (!actualMatcher.find()) {
						throw new Exception("no actual cost in " + 
								startLine);
					}
					String actual = actualMatcher.group();
					// Extract row count from actual measures
					Pattern countPattern = Pattern.compile(
							"rows=\\d+[\\s|\\)]");
					Matcher countMatcher = countPattern.matcher(actual);
					if (!countMatcher.find()) {
						throw new Exception("no row count in " + actual);
					}
					String rowCount = countMatcher.group();
					String countStr = rowCount.substring(5).trim();
					Integer count = Integer.parseInt(countStr);
					// Take into account additional null row
					relToCard.put(thisRel, count);
				}
				//System.out.println(relToCard.toString());
			}
		} else {
			// Extract information starting from next line
			thisRel.or(extractInfoRec(resultLines, start+1, end));
		}
		return thisRel;
	}
	/**
	 * Adds relations whose cardinality is verified based
	 * on the extraction results. A relation's cardinality
	 * is verified if the associated plan was executed,
	 * the cardinality is below the limit, and all
	 * relations appearing in the plan sub-tree are
	 * verified as well.
	 * 
	 * @param plan		verify relations appearing in this plan
	 * @param limit		upper cardinality bound for intermediate results
	 * @param capped		if cardinality is capped by upstream operator
	 * @return			if result generated by input plan is verified
	 */
	CardStatus verifyRec(ProbePlan plan, int limit) throws Exception {
		// Initialize cardinality status
		CardStatus cardStatus = CardStatus.EXACT;
		// Test for join plans (which require recursive calls)
		if (plan instanceof CompositePlan) {
			// this plan joins results
			CompositePlan compositePlan = (CompositePlan)plan;
			ProbePlan leftPlan = compositePlan.leftPlan;
			ProbePlan rightPlan = compositePlan.rightPlan;
			BitSet leftRel = leftPlan.resultRel;
			BitSet rightRel = rightPlan.resultRel;
			CardStatus statusLeft = verifyRec(leftPlan, limit);
			CardStatus statusRight = verifyRec(rightPlan, limit);
			// Treat special case: due to early termination in
			// one of the sub-plans, its result cardinality
			// represents only a lower bound.
			if (leftRel.cardinality() > 1) {
				double leftCard = relToCard.get(leftRel);
				if (leftCard <= 0) {
					markAsBounds(rightPlan);
				}				
			}
			if (rightRel.cardinality() > 1) {
				double rightCard = relToCard.get(rightRel);
				if (rightCard <= 0) {
					markAsBounds(leftPlan);
				}				
			}
			// Calculate cardinality status recursively
			if (statusLeft == CardStatus.LOWER_BOUND || 
					statusRight == CardStatus.LOWER_BOUND) {
				cardStatus = CardStatus.LOWER_BOUND;
			}
			int card = relToCard.get(plan.resultRel);
			if (card < 0) {
				cardStatus = CardStatus.UNKNOWN;
			} else if (card >= limit) {
				cardStatus = CardStatus.LOWER_BOUND;
				// Postgres may not fully execute sub-plans if
				// the row limit is satisfied.
				markAsBounds(plan);
			}
		}
		// Save cardinality status
		relToCardStatus.put(plan.resultRel, cardStatus);
		return cardStatus;
	}
	/**
	 * Marks cardinality of all intermediate results in
	 * given plan as lower bounds in case that they are
	 * currently marked as exact.
	 * 
	 * @param root	root of probe plan
	 */
	void markAsBounds(ProbePlan root) {
		BitSet rel = root.resultRel;
		if (relToCardStatus.get(rel) == CardStatus.EXACT) {
			relToCardStatus.put(rel, CardStatus.LOWER_BOUND);
		}
		if (root instanceof CompositePlan) {
			CompositePlan compositePlan = (CompositePlan)root;
			markAsBounds(compositePlan.leftPlan);
			markAsBounds(compositePlan.rightPlan);
		}
	}
}