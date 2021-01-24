package reference;

import java.util.List;

/**
 * Represents a join predicate between two relations.
 * 
 * @author immanueltrummer
 *
 */
public class JSONjoin {
	/**
	 * Names of two relations linked by the predicate.
	 */
	private List<String> relations;
	/**
	 * Indicates relation on the primary key side.
	 */
	private String primaryKeySide;
	
	public List<String> getRelations() {
		return relations;
	}
	
	public void setRelations(List<String> relations) {
		this.relations = relations;
	}
	
	public String toString() {
		return relations.toString();
	}

	public String getPrimaryKeySide() {
		return primaryKeySide;
	}

	public void setPrimaryKeySide(String primaryKeySide) {
		this.primaryKeySide = primaryKeySide;
	}
}
