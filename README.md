# LeadServiceBase - Brief Mention of Additional Source Types

## 📋 WHAT TO ADD - ONE ROW ONLY

### **Overview Sheet** - Add Row 16 (After Row 15)

**Copy and paste this into Row 16:**

```
Stage 11: Additional Source Types (Databricks Only - New Features)
Hadoop Flow Description: Not applicable - These source types do not exist in Hadoop
Databricks Flow Description: pl_leadservicebase Switch activity also executes additional child pipelines for new source types not present in Hadoop: HFC (hfc_xref_lsb), MH (mh_xref_lsb), CHC (chc_xref_lsb), and Family Clustering variants (es_lsb_famc, fc_lsb_famc, ie_lsb_famc). These are new features added in Databricks that create additional LSB records.
Match Status: Databricks Only
Notes/Comments: These additional source types are executed via child pipelines from pl_leadservicebase but are not part of the detailed comparison scope. They result in additional LSB records in Databricks that do not exist in Hadoop. Detailed analysis focuses on pl_leadservicebase and pl_leadservicebase_gmrnmerge only.
```

---

## ✅ THAT'S IT!

**Only ONE row to add:**
- **Sheet:** Overview
- **Row:** 16 (after Row 15)
- **Purpose:** Brief mention that additional source types exist but are not detailed

**No other sheets need updates** - the detailed comparison focuses on the two requested pipelines only.

---

## 📝 Quick Copy-Paste Instructions

1. Open Excel file: `LeadServiceBase_Comparison_Part5_20251224_143853.xlsx`
2. Go to **Overview** sheet
3. Insert a new row after Row 15
4. Copy the text above (starting from "Stage 11:")
5. Paste into the new row:
   - Column 1: `Stage 11: Additional Source Types (Databricks Only - New Features)`
   - Column 2: `Not applicable - These source types do not exist in Hadoop`
   - Column 3: `pl_leadservicebase Switch activity also executes additional child pipelines for new source types not present in Hadoop: HFC (hfc_xref_lsb), MH (mh_xref_lsb), CHC (chc_xref_lsb), and Family Clustering variants (es_lsb_famc, fc_lsb_famc, ie_lsb_famc). These are new features added in Databricks that create additional LSB records.`
   - Column 4: `Databricks Only`
   - Column 5: `These additional source types are executed via child pipelines from pl_leadservicebase but are not part of the detailed comparison scope. They result in additional LSB records in Databricks that do not exist in Hadoop. Detailed analysis focuses on pl_leadservicebase and pl_leadservicebase_gmrnmerge only.`

**Done!** ✅
