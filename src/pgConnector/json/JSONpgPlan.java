package pgConnector.json;

import java.util.List;

/**
 * Represents one node in plan tree returned
 * by Postgres for an explain query.
 * 
 * @author immanueltrummer
 *
 */
public class JSONpgPlan {
	private String nodetype;
	private String strategy;
	private String partialmode;
	private String parentrelationship;
	private boolean parallelaware;
	private String scandirection;
	private String indexname;
	private String jointype;
	private String relationname;
	private String alias;
	private double startupcost;
	private double totalcost;
	private double planrows;
	private double planwidth;
	private List<String> sortkey;
	private int workersplanned;
	private String recheckcond;
	private boolean singlecopy;
	private boolean innerunique;
	private String mergecond;
	private String hashcond;
	private String indexcond;
	private String filter;
	private String joinfilter;
	
	private List<JSONpgPlan> plans;
	
	public String getNodetype() {
		return nodetype;
	}
	public void setNodetype(String nodetype) {
		this.nodetype = nodetype;
	}
	public boolean isParallelaware() {
		return parallelaware;
	}
	public void setParallelaware(boolean parallelaware) {
		this.parallelaware = parallelaware;
	}
	public String getJointype() {
		return jointype;
	}
	public void setJointype(String jointype) {
		this.jointype = jointype;
	}
	public String getRelationname() {
		return relationname;
	}
	public void setRelationname(String relationName) {
		this.relationname = relationName;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public double getStartupcost() {
		return startupcost;
	}
	public void setStartupcost(double startupcost) {
		this.startupcost = startupcost;
	}
	public double getTotalcost() {
		return totalcost;
	}
	public void setTotalcost(double totalcost) {
		this.totalcost = totalcost;
	}
	public double getPlanrows() {
		return planrows;
	}
	public void setPlanrows(double planrows) {
		this.planrows = planrows;
	}
	public double getPlanwidth() {
		return planwidth;
	}
	public void setPlanwidth(double planwidth) {
		this.planwidth = planwidth;
	}
	public boolean isInnerunique() {
		return innerunique;
	}
	public void setInnerunique(boolean innerunique) {
		this.innerunique = innerunique;
	}
	public List<JSONpgPlan> getPlans() {
		return plans;
	}
	public void setPlans(List<JSONpgPlan> plans) {
		this.plans = plans;
	}
	public String getParentrelationship() {
		return parentrelationship;
	}
	public void setParentrelationship(String parentrelationship) {
		this.parentrelationship = parentrelationship;
	}
	public String getJoinfilter() {
		return joinfilter;
	}
	public void setJoinfilter(String joinfilter) {
		this.joinfilter = joinfilter;
	}
	public int getWorkersplanned() {
		return workersplanned;
	}
	public void setWorkersplanned(int workersplanned) {
		this.workersplanned = workersplanned;
	}
	public String getStrategy() {
		return strategy;
	}
	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}
	public String getPartialmode() {
		return partialmode;
	}
	public void setPartialmode(String partialmode) {
		this.partialmode = partialmode;
	}
	public boolean isSinglecopy() {
		return singlecopy;
	}
	public void setSinglecopy(boolean singlecopy) {
		this.singlecopy = singlecopy;
	}
	public String getHashcond() {
		return hashcond;
	}
	public void setHashcond(String hashcond) {
		this.hashcond = hashcond;
	}
	public String getIndexcond() {
		return indexcond;
	}
	public void setIndexcond(String indexcond) {
		this.indexcond = indexcond;
	}
	public String getScandirection() {
		return scandirection;
	}
	public void setScandirection(String scandirection) {
		this.scandirection = scandirection;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	public String getIndexname() {
		return indexname;
	}
	public void setIndexname(String indexname) {
		this.indexname = indexname;
	}
	public String getRecheckcond() {
		return recheckcond;
	}
	public void setRecheckcond(String recheckcond) {
		this.recheckcond = recheckcond;
	}
	public String getMergecond() {
		return mergecond;
	}
	public void setMergecond(String mergecond) {
		this.mergecond = mergecond;
	}
	public List<String> getSortkey() {
		return sortkey;
	}
	public void setSortkey(List<String> sortkey) {
		this.sortkey = sortkey;
	}
}
