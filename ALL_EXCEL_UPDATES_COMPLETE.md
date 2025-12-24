# Complete Excel Sheet Updates - Copy-Paste Ready Content

## Evidence for HFC Source Type

**✅ CONFIRMED EVIDENCE:**

**Databricks Code** (`process_leads.py:294-295,300`):
```python
all_leads_hfc = all_leads.filter(upper(all_leads.lsb_xrefsourcevalue) == 'GH')
edi_hfc = df.join(all_leads_hfc, ...).select(all_leads_hfc['*'], df.edidatasourcefk)
edi = edi_ich.union(edi_fc).union(edi_chc).union(edi_fm).union(edi_hfc)  # 5 source types
```

**Hadoop Code** (`process_leads.py:480-488`):
```python
# Only 4 source types - NO HFC
edi = edi_ich.union(edi_fc).union(edi_chc).union(edi_fm)  # 4 source types only
```

---

## 1. STTM Comparison Sheet - edidatasourcefk Row

**FIND:** Row with `edidatasourcefk` in first column

**REPLACE ENTIRE ROW WITH:**

```
edidatasourcefk | Source: mappededidatasourcefk lookup table (MapR DB in Hadoop, Delta Lake Parquet in Databricks) - edidatasourcefk field. Processing: Join based on leadsource/xrefsource/associationsource combination. Hadoop processes 4 source types: ICH (lsb_leadsourcevalue='ICH'), FC (lsb_xrefsourcevalue='FC'), CHC (lsb_leadsourcevalue='CHC'), FM (lsb_xrefsourcevalue='FM'). The edidatasourcefk value is retrieved from the lookup table based on matching source combinations. NO hardcoded values are used for globalmrn_assign pipeline. | Source: mappededidatasourcefk lookup table (Delta Lake Parquet ADLS) - edidatasourcefk field. Processing: Join based on leadsource/xrefsource/associationsource combination. Processes 5 source types: ICH (lsb_leadsourcevalue='ICH'), FC (lsb_xrefsourcevalue='FC'), CHC (lsb_leadsourcevalue='CHC'), FM (lsb_xrefsourcevalue='FM'), and HFC (lsb_xrefsourcevalue='GH'). The edidatasourcefk value is retrieved from the lookup table based on matching source combinations. NO hardcoded values are used for globalmrn_assign pipeline. | Similar | Both use lookup tables (not hardcoded values). Databricks includes additional HFC source type (lsb_xrefsourcevalue='GH') not present in Hadoop. Storage differs: MapR DB (Hadoop) vs Parquet (Databricks).
```

---

## 2. Databricks Logic Sheet - Step 13 (addedidatasourcefk)

**FIND:** Text containing "Step 13: Adds edidatasourcefk" and "hardcoded value '49'"

**REPLACE WITH:**

```
Step 13: Adds edidatasourcefk - Reads mappededidatasourcefk lookup table from Delta table (Parquet format). Filters leads by 5 source types: ICH (lsb_leadsourcevalue='ICH'), FC (lsb_xrefsourcevalue='FC'), CHC (lsb_leadsourcevalue='CHC'), FM (lsb_xrefsourcevalue='FM'), and HFC (lsb_xrefsourcevalue='GH'). Joins each filtered lead set with lookup table based on leadsource/xrefsource/associationsource combination to retrieve edidatasourcefk value. Unions all 5 source types. NO hardcoded values - all edidatasourcefk values come from lookup table.
```

---

## 3. Logic Comparison Sheet

**ADD NEW ROW OR UPDATE EXISTING ROW:**

**Column 1 (Step/Function):**
```
Source Type Processing in addedidatasourcefk
```

**Column 2 (Hadoop Logic):**
```
Processes 4 source types: ICH (lsb_leadsourcevalue='ICH'), FC (lsb_xrefsourcevalue='FC'), CHC (lsb_leadsourcevalue='CHC'), FM (lsb_xrefsourcevalue='FM'). Each source type is filtered and joined with mappededidatasourcefk lookup table (MapR DB) to retrieve edidatasourcefk value. NO hardcoded values.
```

**Column 3 (Databricks Logic):**
```
Processes 5 source types: ICH, FC, CHC, FM (same as Hadoop), plus HFC (lsb_xrefsourcevalue='GH'). Each source type is filtered and joined with mappededidatasourcefk lookup table (Parquet) to retrieve edidatasourcefk value. NO hardcoded values.
```

**Column 4 (Comparison Result):**
```
Difference
```

**Column 5 (Note):**
```
Databricks includes additional HFC source type processing. This means Databricks will process leads with lsb_xrefsourcevalue='GH' that Hadoop did not process. Both systems use lookup tables - no hardcoded values for globalmrn_assign.
```

---

## 4. Hadoop Logic Sheet - addedidatasourcefk Section

**FIND:** Section describing `addedidatasourcefk` function

**ENSURE IT STATES (or ADD if missing):**

```
addedidatasourcefk function:
- Processes 4 source types: ICH, FC, CHC, FM
- Source type filters:
  * ICH: lsb_leadsourcevalue == 'ICH'
  * FC: lsb_xrefsourcevalue == 'FC'
  * CHC: lsb_leadsourcevalue == 'CHC'
  * FM: lsb_xrefsourcevalue == 'FM'
- Joins each filtered set with mappededidatasourcefk lookup table (MapR DB)
- Retrieves edidatasourcefk value from lookup table based on leadsource/xrefsource/associationsource match
- Unions all 4 source types
- NO hardcoded values - all edidatasourcefk values come from lookup table
```

---

## 5. Overview Sheet

**FIND:** Section listing key differences

**ENSURE IT INCLUDES:**

```
Key Differences:
1. Source Type Processing: Databricks processes 5 source types (ICH, FC, CHC, FM, HFC) vs Hadoop's 4 source types (ICH, FC, CHC, FM). HFC is identified by lsb_xrefsourcevalue='GH' and is not processed in Hadoop.
2. vsnapflags Filter: Databricks uses 90 days lookback vs Hadoop's 3 days
3. Additional Filters: Databricks includes BCBS prefix, billing deadline, and known coverages filters not present in Hadoop
4. edidatasourcefk Assignment: Both use lookup tables (mappededidatasourcefk) - NO hardcoded values. Databricks processes HFC leads in addition to the 4 source types processed by Hadoop.
```

---

## Summary Checklist

- [ ] **STTM Comparison Sheet**: Update edidatasourcefk row - remove hardcoded values, add HFC mention
- [ ] **Databricks Logic Sheet**: Update Step 13 - remove "hardcoded value '49'", add HFC source type
- [ ] **Logic Comparison Sheet**: Add/update row showing 4 vs 5 source types difference
- [ ] **Hadoop Logic Sheet**: Verify/update to clearly state 4 source types
- [ ] **Overview Sheet**: Verify HFC mention is accurate and complete

---

## Evidence File References

**HFC Evidence:**
- Databricks: `CodebaseIntelligence/Databricks_repo/LeadDiscovery/common/process_leads.py:294-295,300`
- Hadoop: `CodebaseIntelligence/hadoop_repos/hadoop_repos/app-lead-discovery/workflows/leadlookup/spark/process_leads.py:480-488`

**Lookup Table Evidence:**
- Hadoop: Line 479 - `df = spark.loadFromMapRDB(hdfs_maprdb_input)`
- Databricks: Line 288 - `df = spark.read.parquet(hdfs_maprdb_input)`
- Both: Join with `df` to get `df.edidatasourcefk` - NO hardcoded values

