package benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import SQLtools.QueryInfo;
import common.QueryIO;
import optimizers.safe.SafeNaive2;
import pgConnector.PgConnector;
import reference.RefUtil;

public class RunBenchmark {

	/**
	 * 
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Integer startCase = Integer.parseInt(args[0]);
		boolean runOnServer = false;
		// Open result file and write header line
		int runNr = 6;
		String experimentsDir = runOnServer ? "":
				"/Users/immanueltrummer/Documents/Papers/" + 
				"verifiedOptimization/experiments/";
		File resultFile = new File(experimentsDir + 
				"job" + runNr + ".tsv");
		PrintWriter resultWriter = new PrintWriter(resultFile);
		resultWriter.println("query\ttestNr\toptimizer\tmillis" +
				"\ttimeout\texception\tnrQueries");
		// Initialize benchmark-related variables
		List<Boolean> timeouts = new ArrayList<Boolean>();
		List<Long> millis = new ArrayList<Long>();
		// Establish connection to the database
		PgConnector pgConnector = new PgConnector(
				runOnServer?"postgres":"immanueltrummer",
				"", runOnServer?"imdb":"imdbload");
		// Initialize all compared algorithms
		int timeoutMillis = 7200000;
		SafeNaive2 safeNaive = new SafeNaive2(timeoutMillis, pgConnector);
		// Iterate over input queries
		int testCaseCtr = 0;
		for (int templateCtr=1; templateCtr<=33; ++templateCtr) {
			for (char variant : new char[] {'a', 'b', 'c', 'd', 'e', 'f'}) {
			//for (char variant : new char[] {'b'}) {
				// Generate path for potential query
				String queryPath = experimentsDir + 
						"input/job/queries/" +
						templateCtr + variant + ".sql";
				File queryFile = new File(queryPath);
				if (queryFile.exists()) {
					// Advance test case number
					++testCaseCtr;
					if (testCaseCtr >= startCase) {
						// Initialize query-related objects
						System.out.println("Query" + templateCtr + variant);
						System.out.println("aka testcase nr. " + testCaseCtr);
						String query = QueryIO.readQuery(queryPath);
						QueryInfo queryInfo = new QueryInfo(query);
						RefUtil refUtil = new RefUtil(experimentsDir, 
								testCaseCtr, queryInfo);
						String queryName = "q" + templateCtr + variant;
						// Try out safe optimizer
						boolean safeException = false;
						try {
							safeNaive.optimize(queryInfo, refUtil);
							System.out.println(queryInfo.toString());
							System.out.println("Query " + templateCtr + variant);
							refUtil.testRelInfos(safeNaive.relInfos, safeNaive.timeout, 0.01);
							millis.add(safeNaive.totalMillis);
							timeouts.add(safeNaive.timeout);							
						} catch (Exception e) {
							safeException = true;
							//throw e;
						}
						// Serialize optimizer result to file
						String relInfoPath = experimentsDir + 
								"cardResult" + testCaseCtr;
						FileOutputStream relInfoFileStream = 
								new FileOutputStream(relInfoPath);
						ObjectOutputStream relInfoObjStream =
								new ObjectOutputStream(relInfoFileStream);
						relInfoObjStream.writeObject(safeNaive.relInfos);
						relInfoObjStream.close();
						relInfoFileStream.close();
						// Write to benchmark file
						resultWriter.print(queryName + "\t");
						resultWriter.print(testCaseCtr + "\t");
						resultWriter.print("safe\t");
						resultWriter.print(safeNaive.totalMillis + "\t");
						resultWriter.print(safeNaive.timeout + "\t");
						resultWriter.print(safeException + "\t");
						resultWriter.println(safeNaive.nrQueries);
						resultWriter.flush();
					}
				}
				System.out.println(millis.toString());
				System.out.println(timeouts.toString());
			}
		}
		System.out.println("Time measurements");
		System.out.println(millis.toString());
		System.out.println(timeouts.toString());
		resultWriter.close();
		pgConnector.closeAll();
	}
	
}
