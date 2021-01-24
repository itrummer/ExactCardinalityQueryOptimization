package SQLtools;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * Extracts meta-data from the WHERE clause of an input query.
 * 
 * @author immanueltrummer
 *
 */
public class PredicateExtractor extends TablesNamesFinder {
	/**
	 * Stack containing predicates extracted from query sub-trees.
	 */
	public final Stack<PredInfo> predStack = new Stack<PredInfo>();
	/**
	 * Stack containing table IDs extracted from query sub-trees.
	 */
	public final Stack<Integer> tableIDstack = new Stack<Integer>();
	/**
	 * Stack containing column references extracted from sub-queries.
	 */
	public final Stack<Column> columnStack = new Stack<Column>();
	/**
	 * Maps table alias to table IDs.
	 */
	public final Map<String, Integer> tableAliasToID;
	/**
	 * Maps table alias to the set of its columns that
	 * are referenced in query predicates. 
	 */
	public final Map<String, Set<String>> aliasToPredCols =
			new HashMap<String, Set<String>>();
	/**
	 * Retrieves mapping from table aliases to table IDs.
	 * 
	 * @param tableAliasToID	maps table aliases to table IDs
	 */
	public PredicateExtractor(Map<String, Integer> tableAliasToID) {
		this.tableAliasToID = tableAliasToID;
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		if (plainSelect.getWhere() != null) {
			plainSelect.getWhere().accept(this);
		}
	}
	
	@Override
	public void visit(SetOperationList setOpList) {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void visit(WithItem withItem) {
		// TODO Auto-generated method stub
	}
	
	public void visit(Expression expression) {
		
	}
	
	@Override
	public void visit(BitwiseRightShift aThis) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(BitwiseLeftShift aThis) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(NullValue nullValue) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Function function) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(SignedExpression signedExpression) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(JdbcParameter jdbcParameter) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(JdbcNamedParameter jdbcNamedParameter) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(DoubleValue doubleValue) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(LongValue longValue) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(HexValue hexValue) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(DateValue dateValue) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(TimeValue timeValue) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(TimestampValue timestampValue) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Parenthesis parenthesis) {
		super.visit(parenthesis);
		
	}
	@Override
	public void visit(StringValue stringValue) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Addition addition) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Division division) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Multiplication multiplication) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Subtraction subtraction) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(AndExpression andExpression) {
		super.visit(andExpression);
		predStack.add(new CompositePredInfo(
				PredConnector.AND, 
				predStack.pop(), predStack.pop()));
	}
	@Override
	public void visit(OrExpression orExpression) {
		super.visit(orExpression);
		predStack.add(new CompositePredInfo(
				PredConnector.OR, 
				predStack.pop(), predStack.pop()));
	}
	@Override
	public void visit(Between between) {
		super.visit(between);
		predStack.add(new PredInfo(between.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(EqualsTo equalsTo) {
		super.visit(equalsTo);
		//System.out.println("equalsTO" + equalsTo.toString());
		predStack.add(new PredInfo(equalsTo.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(GreaterThan greaterThan) {
		super.visit(greaterThan);
		predStack.add(new PredInfo(greaterThan.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		super.visit(greaterThanEquals);
		predStack.add(new PredInfo(greaterThanEquals.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(InExpression inExpression) {
		super.visit(inExpression);
		predStack.add(new PredInfo(inExpression.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(IsNullExpression isNullExpression) {
		// (super class does nothing in the current version)
		super.visit(isNullExpression);
		isNullExpression.getLeftExpression().accept(this);
		predStack.add(new PredInfo(isNullExpression.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(LikeExpression likeExpression) {
		super.visit(likeExpression);
		predStack.add(new PredInfo(likeExpression.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(MinorThan minorThan) {
		super.visit(minorThan);
		predStack.add(new PredInfo(minorThan.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		super.visit(minorThanEquals);
		predStack.add(new PredInfo(minorThanEquals.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		super.visit(notEqualsTo);
		predStack.add(new PredInfo(notEqualsTo.toString(), 
				tableIDstack, columnStack));
	}
	@Override
	public void visit(Column tableColumn) {
		String alias = tableColumn.getTable().getName();
		Integer tableID = tableAliasToID.get(alias);
		tableIDstack.add(tableID);
		String column = tableColumn.getColumnName();
		Set<String> columns = aliasToPredCols.getOrDefault(
				alias, new HashSet<String>());
		columns.add(column);
		aliasToPredCols.put(alias, columns);
		columnStack.add(tableColumn);
	}
	@Override
	public void visit(SubSelect subSelect) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(CaseExpression caseExpression) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(WhenClause whenClause) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(ExistsExpression existsExpression) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Concat concat) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Matches matches) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(BitwiseOr bitwiseOr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(BitwiseXor bitwiseXor) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(CastExpression cast) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(Modulo modulo) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(AnalyticExpression aexpr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(ExtractExpression eexpr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(IntervalExpression iexpr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(OracleHierarchicalExpression oexpr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(RegExpMatchOperator rexpr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(JsonExpression jsonExpr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(JsonOperator jsonExpr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(RegExpMySQLOperator regExpMySQLOperator) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(UserVariable var) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(NumericBind bind) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(KeepExpression aexpr) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(MySQLGroupConcat groupConcat) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(ValueListExpression valueList) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(RowConstructor rowConstructor) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(OracleHint hint) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(TimeKeyExpression timeKeyExpression) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(DateTimeLiteralExpression literal) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(NotExpression aThis) {
		//System.out.println("Visiting not expression");
		super.visit(aThis);
		
	}
}
