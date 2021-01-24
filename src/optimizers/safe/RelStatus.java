package optimizers.safe;

/**
 * Status of a relation with regards to
 * verified optimization.
 * 
 * @author immanueltrummer
 *
 */
public enum RelStatus {
	PENDING,		// no final status known yet
	VERIFIED,	// exact cardinality is verified
	UNVERIF,		// cannot be verified with current budget
	EXCLUDED		// cannot be part of any optimal plan
}
