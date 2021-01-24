package reference;


/**
 * Info on single relation retrieved from JSON file.
 * 
 * @author immanueltrummer
 *
 */
public class JSONrelation {
	/**
	 * Name of the relation.
	 */
	private String name;
	/**
	 * Set if relation is no base table.
	 */
	private String baseTable;
	/**
	 * Lower case version of baseTable, used in some of the files.
	 */
	private String basetable;
	/**
	 * Cardinality of this table after filtering.
	 */
	private long cardinality;
	/**
	 * Cardinality of this table before filtering.
	 */
	private long unfilteredCardinality;
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getBaseTable() {
		return baseTable;
	}
	
	public void setBaseTable(String baseTable) {
		this.baseTable = baseTable;
	}
	
	public long getCardinality() {
		return cardinality;
	}
	
	public void setCardinality(long cardinality) {
		this.cardinality = cardinality;
	}
	
	public long getUnfilteredCardinality() {
		return unfilteredCardinality;
	}
	
	public void setUnfilteredCardinality(long unfilteredCardinality) {
		this.unfilteredCardinality = unfilteredCardinality;
	}
	
	public String toString() {
		return getName() + ", " + getBaseTable() + ", " + 
				getCardinality() + ", " + getUnfilteredCardinality();
	}

	public String getBasetable() {
		return basetable;
	}

	public void setBasetable(String basetable) {
		this.basetable = basetable;
	}
}
