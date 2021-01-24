package common;

import static org.junit.jupiter.api.Assertions.*;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

import SQLtools.QueryInfo;

class PlanUtilTest {

	@Test
	void test() throws Exception {
		// Verify set of generated relations
		{
			// Test with query 1a of JOB
			QueryInfo queryInfo = new QueryInfo("SELECT MIN(mc.note) AS" + 
					" production_note, MIN(t.title) AS movie_title, " + 
					"MIN(t.production_year) AS movie_year FROM company_type AS ct, " + 
					"info_type AS it, movie_companies AS mc, movie_info_idx AS mi_idx," + 
					" title AS t WHERE ct.kind = 'production companies' AND " + 
					"it.info = 'top 250 rank' AND mc.note  not like '%(as Metro-Goldwyn-Mayer Pictures)%' " + 
					"and (mc.note like '%(co-production)%' or mc.note like '%(presents)%') " + 
					"AND ct.id = mc.company_type_id AND t.id = mc.movie_id AND " + 
					"t.id = mi_idx.movie_id AND mc.movie_id = mi_idx.movie_id AND " + 
					"it.id = mi_idx.info_type_id;\n");
			// Obtain table IDs for easy verification
			Integer ctID = queryInfo.tableAliasToID.get("ct");
			Integer mcID = queryInfo.tableAliasToID.get("mc");
			Integer tID = queryInfo.tableAliasToID.get("t");
			Integer mi_idxID = queryInfo.tableAliasToID.get("mi_idx");
			Integer itID = queryInfo.tableAliasToID.get("it");
			/*
			 * Query predicates: ct-mc, t-mc, t-mi_idx, mc-mi_idx, it-mi_idx.
			 */
			PlanUtil planUtil = new PlanUtil(queryInfo);
			// Create singleton relations
			BitSet ctRel = new BitSet();
			BitSet mcRel = new BitSet();
			BitSet tRel = new BitSet();
			BitSet mi_idxRel = new BitSet();
			BitSet itRel = new BitSet();
			ctRel.set(ctID);
			mcRel.set(mcID);
			tRel.set(tID);
			mi_idxRel.set(mi_idxID);
			itRel.set(itID);
			assertTrue(planUtil.allRels.contains(ctRel));
			assertTrue(planUtil.allRels.contains(mcRel));
			assertTrue(planUtil.allRels.contains(tRel));
			assertTrue(planUtil.allRels.contains(mi_idxRel));
			assertTrue(planUtil.allRels.contains(itRel));
			// Create binary relations
			BitSet ctMcRel = new BitSet();
			BitSet tMcRel = new BitSet();
			BitSet tMi_idxRel = new BitSet();
			BitSet mcMi_idxRel = new BitSet();
			BitSet itMi_idxRel = new BitSet();
			ctMcRel.or(ctRel);
			ctMcRel.or(mcRel);
			tMcRel.or(tRel);
			tMcRel.or(mcRel);
			tMi_idxRel.or(tRel);
			tMi_idxRel.or(mi_idxRel);
			mcMi_idxRel.or(mcRel);
			mcMi_idxRel.or(mi_idxRel);
			itMi_idxRel.or(itRel);
			itMi_idxRel.or(mi_idxRel);
			assertTrue(planUtil.allRels.contains(ctMcRel));
			assertTrue(planUtil.allRels.contains(tMcRel));
			assertTrue(planUtil.allRels.contains(tMi_idxRel));
			assertTrue(planUtil.allRels.contains(mcMi_idxRel));
			assertTrue(planUtil.allRels.contains(itMi_idxRel));
			// Generate relations with three tables
			BitSet ctMcTRel = new BitSet();
			BitSet ctMcMi_idxRel = new BitSet();
			BitSet tMcMi_idxRel = new BitSet();
			BitSet tMi_idxItRel = new BitSet();
			BitSet mcMi_idxItRel = new BitSet();
			ctMcTRel.or(ctMcRel);
			ctMcTRel.or(tRel);
			ctMcMi_idxRel.or(ctMcRel);
			ctMcMi_idxRel.or(mi_idxRel);
			tMcMi_idxRel.or(tMcRel);
			tMcMi_idxRel.or(mi_idxRel);
			tMi_idxItRel.or(tMi_idxRel);
			tMi_idxItRel.or(itRel);
			mcMi_idxItRel.or(mcMi_idxRel);
			mcMi_idxItRel.or(itRel);
			assertTrue(planUtil.allRels.contains(ctMcTRel));
			assertTrue(planUtil.allRels.contains(ctMcMi_idxRel));
			assertTrue(planUtil.allRels.contains(tMcMi_idxRel));
			assertTrue(planUtil.allRels.contains(tMi_idxItRel));
			assertTrue(planUtil.allRels.contains(mcMi_idxItRel));
			// Generate relations with four tables
			BitSet ctMcTMi_idxRel = new BitSet();
			BitSet ctMcMi_idxItRel = new BitSet();
			BitSet tMcMi_idxItRel = new BitSet();
			ctMcTMi_idxRel.or(ctMcTRel);
			ctMcTMi_idxRel.or(mi_idxRel);
			ctMcMi_idxItRel.or(ctMcMi_idxRel);
			ctMcMi_idxItRel.or(itRel);
			tMcMi_idxItRel.or(tMcMi_idxRel);
			tMcMi_idxItRel.or(itRel);
			assertTrue(planUtil.allRels.contains(ctMcTMi_idxRel));
			assertTrue(planUtil.allRels.contains(ctMcMi_idxItRel));
			assertTrue(planUtil.allRels.contains(tMcMi_idxItRel));
			// Generate one relation with all tables
			BitSet allTablesRel = new BitSet();
			allTablesRel.or(tMcMi_idxItRel);
			allTablesRel.or(ctRel);
			assertTrue(planUtil.allRels.contains(allTablesRel));
			// Verify the number of generated relations
			assertEquals(5, planUtil.relsByCard.get(1).size());
			assertEquals(19, planUtil.allRels.size());
		}
	}

}
