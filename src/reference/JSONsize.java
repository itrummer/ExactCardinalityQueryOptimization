package reference;

import java.util.List;

/**
 * Represents the size of the join between two relations
 * connected via a join predicate.
 * 
 * @author immanueltrummer
 *
 */
public class JSONsize {
	/**
	 * Two relations connected via a predicate.
	 */
	private List<String> relations;
	/**
	 * Cardinality of joined relations (after applying predicates).
	 */
	private double cardinality;
	
	public List<String> getRelations() {
		return relations;
	}
	
	public void setRelations(List<String> relations) {
		this.relations = relations;
	}

	public double getCardinality() {
		return cardinality;
	}

	public void setCardinality(double cardinality) {
		this.cardinality = cardinality;
	}
	
	public String toString() {
		return relations.toString() + ", " + getCardinality();
	}
}
