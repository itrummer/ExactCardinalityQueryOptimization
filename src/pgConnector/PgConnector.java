package pgConnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Contains low-level methods for issuing
 * queries to Postgres.
 * 
 * @author immanueltrummer
 *
 */
public class PgConnector {
	/**
	 * URL pointing to current database.
	 */
	final String url;
	/**
	 * Connection to database system (initialized
	 * in constructor).
	 */
	final Connection connection;
	/**
	 * Statement used for queries (initialized
	 * in constructor).
	 */
	final Statement statement;
	/**
	 * Constructor initializes connection to the database.
	 * 
	 * @param user		user name for database access
	 * @param password	password for database access	
	 * @param dbName		name of database to access
	 * 
	 * @throws Exception
	 */
	public PgConnector(String user, String password, 
			String database) throws Exception {
		this.url = "jdbc:postgresql:" + database;
		this.connection = DriverManager.getConnection(url, user, password);
		this.statement = connection.createStatement();
		// Disable nested loop joins following recommendations
		// in paper proposing join order benchmark - if this
		// is removed, the extraction of cardinality estimates
		// from explain-analyze query results needs to be
		// adapted as well.
		statement.execute("set enable_nestloop = false;");
		//statement.execute("set enable_mergejoin = false;");
		//statement.execute("set enable_hashjoin = false;");
	}
	/**
	 * Returns a result set for a given SQL query.
	 * 
	 * @param sql			sql query string
	 * @param timeoutMillis	number of milliseconds until timeout
	 * @return				result set with query result
	 * @throws Exception
	 */
	public ResultSet query(String sql, long timeoutMillis) throws Exception {
		if (timeoutMillis > 0) {
			statement.setQueryTimeout((int)(timeoutMillis / 1000));			
		}
		return statement.executeQuery(sql);
	}
	/**
	 * Extracts a string representation from a query result for one
	 * specified column index.
	 * 
	 * @param result		result of SQL query
	 * @param column		index of column to extract from
	 * @return			a string representation extracted from result
	 */
	public String extractResultString(ResultSet result, int column) throws Exception {
		StringBuilder resultBuilder = new StringBuilder();
		while (result.next()) {
			resultBuilder.append(result.getString(column) + "\n");
		}
		return resultBuilder.toString();
	}
	/**
	 * Closes all connections held by his object.
	 */
	public void closeAll() throws Exception {
		connection.close();
	}
}