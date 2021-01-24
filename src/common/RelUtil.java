package common;

import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Contains helper methods to handle relations.
 * 
 * @author immanueltrummer
 *
 */
public class RelUtil {
	/**
	 * Returns true iff the first BitSet is a subset
	 * of the second BitSet.
	 * 
	 * @param subset		a potential subset		
	 * @param set		a BitSet
	 * @return			true iff it is indeed a subset
	 */
	public static boolean isSubset(BitSet subset, BitSet set) {
		BitSet temp = new BitSet();
		temp.or(subset);
		temp.andNot(set);
		return temp.isEmpty();
	}
	/**
	 * Transforms a relation represented as BitSet into a
	 * set containing the corresponding table aliases.
	 * 
	 * @param rel				relation in BitSet representation
	 * @param tableIDtoAlias		maps table IDs to table aliases
	 * @return					a set of table aliases
	 */
	public static Set<String> aliasSet(BitSet rel, 
			Map<Integer, String> tableIDtoAlias) {
		// Iterate over table IDs in BitSet
		Set<String> aliasSet = new TreeSet<String>();
		for (int table=rel.nextSetBit(0); table>=0; 
				table=rel.nextSetBit(table+1)) {
			String alias = tableIDtoAlias.get(table);
			aliasSet.add(alias);
		}
		return aliasSet;
	}
}
