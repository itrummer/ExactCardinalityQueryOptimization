package reference;

import java.util.List;

/**
 * Query as represented in the JSON format used by Neumann et al.
 * 
 * @author immanueltrummer
 *
 */
public class JSONquery {
	/**
	 * Name of the query.
	 */
	private String name;
	/**
	 * List of base JSONrelations with associated cardinality.
	 */
	private List<JSONrelation> relations;
	/**
	 * List of join predicates, describing tables they connect.
	 */
	private List<JSONjoin> joins;
	/**
	 * Implicit description of join predicate selectivity values.
	 */
	private List<JSONsize> sizes;
	
	public List<JSONrelation> getrelations() {
		return relations;
	}
	public void setrelations(List<JSONrelation> relations) {
		this.relations = relations;
	}
	public List<JSONjoin> getJoins() {
		return joins;
	}
	public void setJoins(List<JSONjoin> joins) {
		this.joins = joins;
	}
	public List<JSONsize> getsizes() {
		return sizes;
	}
	public void setsizes(List<JSONsize> sizes) {
		this.sizes = sizes;
	}
	public String toString() {
		return relations.toString() + ", " + 
				(joins == null? "null" : joins.toString()) + 
				", " + sizes.toString();
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
}
