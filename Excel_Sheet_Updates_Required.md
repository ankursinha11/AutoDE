# Excel Sheet Updates Required - HFC Source Type and edidatasourcefk

## Evidence Summary

**HFC Source Type Evidence:**
- ✅ **Databricks** (`process_leads.py:294-295,300`): Filters for `lsb_xrefsourcevalue == 'GH'` and processes HFC leads
- ✅ **Hadoop** (`process_leads.py:480-488`): Does NOT filter for HFC/GH - only processes ICH, FC, CHC, FM

**edidatasourcefk Evidence:**
- ✅ **Both use lookup tables** - NOT hardcoded values
- ❌ **Excel incorrectly states** hardcoded values (41/49)

---

## 1. STTM Comparison Sheet - edidatasourcefk Row

**Current Status:** ❌ Incorrectly states hardcoded values

**REPLACE WITH THIS EXACT CONTENT:**

```
edidatasourcefk | Source: mappededidatasourcefk lookup table (MapR DB in Hadoop, Delta Lake Parquet in Databricks) - edidatasourcefk field. Processing: Join based on leadsource/xrefsource/associationsource combination. Hadoop processes 4 source types: ICH (lsb_leadsourcevalue='ICH'), FC (lsb_xrefsourcevalue='FC'), CHC (lsb_leadsourcevalue='CHC'), FM (lsb_xrefsourcevalue='FM'). Databricks processes 5 source types: ICH, FC, CHC, FM, and HFC (lsb_xrefsourcevalue='GH'). The edidatasourcefk value is retrieved from the lookup table based on matching source combinations. NO hardcoded values are used for globalmrn_assign pipeline. | Source: mappededidatasourcefk lookup table (Delta Lake Parquet ADLS) - edidatasourcefk field. Processing: Join based on leadsource/xrefsource/associationsource combination. Processes 5 source types: ICH (lsb_leadsourcevalue='ICH'), FC (lsb_xrefsourcevalue='FC'), CHC (lsb_leadsourcevalue='CHC'), FM (lsb_xrefsourcevalue='FM'), and HFC (lsb_xrefsourcevalue='GH'). The edidatasourcefk value is retrieved from the lookup table based on matching source combinations. NO hardcoded values are used for globalmrn_assign pipeline. | Similar | Both use lookup tables (not hardcoded values). Databricks includes additional HFC source type (lsb_xrefsourcevalue='GH') not present in Hadoop. Storage differs: MapR DB (Hadoop) vs Parquet (Databricks).
```

---

## 2. Databricks Logic Sheet - addedidatasourcefk Step

**Current Status:** ⚠️ Mentions "hardcoded value '49'" which is INCORRECT

**FIND THIS TEXT (around Row 11, Step 13):**
```
Step 13: Adds edidatasourcefk - Reads mappededidatasourcefk from Delta table, joins based on leadsource/xrefsource/associationsource for ICH/FC/CHC/FM leads, hardcoded value '49' (vs '41' in Hadoop).
```

**REPLACE WITH:**
```
Step 13: Adds edidatasourcefk - Reads mappededidatasourcefk lookup table from Delta table (Parquet format). Filters leads by source type: ICH (lsb_leadsourcevalue='ICH'), FC (lsb_xrefsourcevalue='FC'), CHC (lsb_leadsourcevalue='CHC'), FM (lsb_xrefsourcevalue='FM'), and HFC (lsb_xrefsourcevalue='GH'). Joins each filtered lead set with lookup table based on leadsource/xrefsource/associationsource combination to retrieve edidatasourcefk value. Unions all 5 source types. NO hardcoded values - all edidatasourcefk values come from lookup table.
```

---

## 3. Logic Comparison Sheet

**Current Status:** ⚠️ May not clearly show HFC difference

**ADD THIS AS A NEW ROW (or update existing row about source types):**

```
Source Type Processing | Hadoop: Processes 4 source types in addedidatasourcefk function: ICH (lsb_leadsourcevalue='ICH'), FC (lsb_xrefsourcevalue='FC'), CHC (lsb_leadsourcevalue='CHC'), FM (lsb_xrefsourcevalue='FM'). Each source type is filtered and joined with mappededidatasourcefk lookup table (MapR DB) to retrieve edidatasourcefk value. | Databricks: Processes 5 source types in addedidatasourcefk function: ICH, FC, CHC, FM (same as Hadoop), plus HFC (lsb_xrefsourcevalue='GH'). Each source type is filtered and joined with mappededidatasourcefk lookup table (Parquet) to retrieve edidatasourcefk value. | Difference | Databricks includes additional HFC source type processing. This means Databricks will process leads with lsb_xrefsourcevalue='GH' that Hadoop did not process. Both systems use lookup tables - no hardcoded values for globalmrn_assign.
```

---

## 4. Hadoop Logic Sheet

**Current Status:** ❓ May not mention source types explicitly

**ADD THIS CONTENT (if not already present):**

**Find the section about addedidatasourcefk function and ensure it states:**

```
addedidatasourcefk function:
- Processes 4 source types: ICH, FC, CHC, FM
- Filters leads by source type:
  * ICH: lsb_leadsourcevalue == 'ICH'
  * FC: lsb_xrefsourcevalue == 'FC'
  * CHC: lsb_leadsourcevalue == 'CHC'
  * FM: lsb_xrefsourcevalue == 'FM'
- Joins each filtered set with mappededidatasourcefk lookup table (MapR DB)
- Retrieves edidatasourcefk value from lookup table based on leadsource/xrefsource/associationsource match
- Unions all 4 source types
- NO hardcoded values - all values come from lookup table
```

---

## 5. Overview Sheet

**Current Status:** ✅ Mentions HFC but verify accuracy

**ENSURE IT STATES:**

```
Key Differences:
1. Source Type Processing: Databricks includes HFC source type (lsb_xrefsourcevalue='GH') not present in Hadoop
2. vsnapflags Filter: Databricks uses 90 days lookback vs Hadoop's 3 days
3. Additional Filters: Databricks includes BCBS prefix, billing deadline, and known coverages filters
4. edidatasourcefk: Both use lookup tables (not hardcoded values) - Databricks processes 5 source types, Hadoop processes 4
```

---

## Evidence Code References

**Databricks HFC Processing:**
- File: `CodebaseIntelligence/Databricks_repo/LeadDiscovery/common/process_leads.py`
- Lines: 294-295 (filter), 300 (union)
- Code: `all_leads_hfc = all_leads.filter(upper(all_leads.lsb_xrefsourcevalue) == 'GH')`
- Code: `edi = edi_ich.union(edi_fc).union(edi_chc).union(edi_fm).union(edi_hfc)`

**Hadoop (No HFC):**
- File: `CodebaseIntelligence/hadoop_repos/hadoop_repos/app-lead-discovery/workflows/leadlookup/spark/process_leads.py`
- Lines: 480-488
- Code: Only filters ICH, FC, CHC, FM (4 types)
- Code: `edi = edi_ich.union(edi_fc).union(edi_chc).union(edi_fm)` (NO edi_hfc)

**Both Use Lookup Tables:**
- Hadoop: `df = spark.loadFromMapRDB(hdfs_maprdb_input)` (line 479)
- Databricks: `df = spark.read.parquet(hdfs_maprdb_input)` (line 288)
- Both join with `df` (lookup table) to get `df.edidatasourcefk` - NO hardcoded values

---

## Summary of Required Updates

1. ✅ **STTM Comparison Sheet** - edidatasourcefk row: Update to reflect lookup table usage and HFC inclusion
2. ✅ **Databricks Logic Sheet** - Step 13: Remove incorrect "hardcoded value '49'" statement, add HFC source type
3. ✅ **Logic Comparison Sheet** - Add/update row showing 4 vs 5 source types difference
4. ✅ **Hadoop Logic Sheet** - Ensure it clearly states 4 source types (if not already)
5. ✅ **Overview Sheet** - Verify HFC mention is accurate

All updates should emphasize:
- **NO hardcoded values** for edidatasourcefk in globalmrn_assign
- **Hadoop: 4 source types** (ICH, FC, CHC, FM)
- **Databricks: 5 source types** (ICH, FC, CHC, FM, HFC)
- **HFC identified by:** `lsb_xrefsourcevalue == 'GH'`

