package SQLtools;

import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * Extracts tables and their aliases from a query string.
 * 
 * @author immanueltrummer
 *
 */
public class TableExtractor extends TablesNamesFinder {
	/**
	 * Maps table alias to actual table name.
	 */
	public final Map<String, String> tableAliasToName;
	/**
	 * Retrieves the target collections to fill via parsing.
	 */
	public TableExtractor() {
		super();
		this.tableAliasToName = new HashMap<String, String>();
	}
	@Override
	public void visit(Table table) {
		String tableName = table.getName();
		String tableAlias = table.getAlias().getName();
		tableAliasToName.put(tableAlias, tableName);
	}
}