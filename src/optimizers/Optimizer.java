package optimizers;

import SQLtools.QueryInfo;
import pgConnector.PgConnector;
import reference.RefUtil;

/**
 * Super-class of all approaches for generating
 * verified-optimal query plans.
 * 
 * @author immanueltrummer
 *
 */
public abstract class Optimizer {
	/**
	 * Allows to query the database.
	 */
	public final PgConnector pgConnector;
	/**
	 * Maximum number of milliseconds before timeout.
	 */
	public final int timeoutMillis;
	/**
	 * The number of milliseconds used in the last invocation.
	 */
	public long totalMillis;
	/**
	 * Whether a timeout occurred (per optimization timeout).
	 */
	public boolean timeout;
	/**
	 * Number of DB queries issued by optimizer during
	 * last invocation.
	 */
	public int nrQueries;
	/**
	 * Calculates number of milliseconds since optimization
	 * start and activates the timeout flag if the number
	 * exceeds the specified timeout.
	 * 
	 * @param startMillis	optimization start time
	 */
	public void updateTime(long startMillis) {
		totalMillis = System.currentTimeMillis() - startMillis;
		if (totalMillis > timeoutMillis) {
			timeout = true;
		}
	}
	/**
	 * Initializes timeout and database connector.
	 * 
	 * @param timeoutMillis	number of milliseconds until timeout
	 * @param pgConnector	connection to the database
	 */
	public Optimizer(int timeoutMillis, PgConnector pgConnector) {
		this.timeoutMillis = timeoutMillis;
		this.pgConnector = pgConnector;
	}
	/**
	 * Find verified-optimal plans for given input query.
	 * 
	 * @param queryInfo	query with associated meta-data
	 * @param reference solution to use for internal testing
	 * @throws Exception
	 */
	public abstract void optimize(QueryInfo queryInfo, 
			RefUtil refUtil) throws Exception;
}
