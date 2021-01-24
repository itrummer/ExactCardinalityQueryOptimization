package benchmark;

import java.io.File;
import java.io.PrintWriter;

import SQLtools.QueryInfo;
import common.QueryIO;
import reference.RefUtil;

/**
 * Generates a file containing statistics about
 * the input query.
 * 
 * @author immanueltrummer
 *
 */
public class GenerateQueryStats {

	public static void main(String[] args) throws Exception {
		boolean runOnServer = false;
		// Open result file and write header line
		String experimentsDir = runOnServer ? "":
				"/Users/immanueltrummer/Documents/Papers/" + 
				"verifiedOptimization/experiments/";
		File resultFile = new File(experimentsDir + "queryStats.tsv");
		PrintWriter resultWriter = new PrintWriter(resultFile);
		resultWriter.println("query\ttestNr\tnrTables\tnrRels\tcardVar");
		// Iterate over input queries
		int testCaseCtr = 0;
		for (int templateCtr=1; templateCtr<=33; ++templateCtr) {
			for (char variant : new char[] {'a', 'b', 'c', 'd', 'e', 'f'}) {
				// Generate path for potential query
				String queryPath = experimentsDir + 
						"input/job/queries/" +
						templateCtr + variant + ".sql";
				File queryFile = new File(queryPath);
				if (queryFile.exists()) {
					// Advance test case number
					++testCaseCtr;
					// Initialize query-related objects
					System.out.println("Query" + templateCtr + variant);
					System.out.println("aka testcase nr. " + testCaseCtr);
					String query = QueryIO.readQuery(queryPath);
					QueryInfo queryInfo = new QueryInfo(query);
					RefUtil refUtil = new RefUtil(experimentsDir, 
							testCaseCtr, queryInfo);
					// Extract query statistics
					int nrTables = queryInfo.nrTables;
					int nrRels = refUtil.refCard.size();
					double cardVar = refUtil.cardVariance(true);
					// Write to benchmark file
					String queryName = "q" + templateCtr + variant;
					resultWriter.print(queryName + "\t");
					resultWriter.print(testCaseCtr + "\t");
					resultWriter.print(nrTables + "\t");
					resultWriter.print(nrRels + "\t");
					resultWriter.println(cardVar);
				}
			}
		}
		resultWriter.close();
	}
}
