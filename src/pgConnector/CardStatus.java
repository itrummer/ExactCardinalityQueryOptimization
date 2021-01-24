package pgConnector;

/**
 * Contains information on how a reported cardinality
 * estimate relates to the true cardinality of a
 * relation.
 * 
 * @author immanueltrummer
 *
 */
public enum CardStatus {
	UNKNOWN,			// no cardinality estimate is known
	LOWER_BOUND,		// the estimate represents a lower bound
	EXACT			// the estimate is exact
}
