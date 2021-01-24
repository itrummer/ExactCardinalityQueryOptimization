package common;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Auxiliary methods for reading queries from hard disc.
 * 
 * @author immanueltrummer
 *
 */
public class QueryIO {
	/**
	 * Reads an SQL query from a text file on disc.
	 * 
	 * @param queryPath	path to query
	 * @return			query read from disc
	 */
	public static String readQuery(String queryPath) throws Exception {
		FileReader fileReader = new FileReader(queryPath);
		BufferedReader queryReader = new BufferedReader(fileReader);
		StringBuffer queryBuffer = new StringBuffer();
		String line;
		while ((line = queryReader.readLine()) != null) {
			queryBuffer.append(line);
		}
		queryReader.close();
		String query = queryBuffer.toString();
		return query;
	}
}
