# LEADLOOKUP GlobalMRN Assign - STTM Corrections and Flow Diagrams

## 1. Corrected edidatasourcefk Row Content

**Copy this exact content to replace the edidatasourcefk row in STTM Comparison sheet:**

```
edidatasourcefk | Source: mappededidatasourcefk lookup table (MapR DB in Hadoop, Delta Lake Parquet in Databricks) - edidatasourcefk field. Processing: Join based on leadsource/xrefsource/associationsource combination for ICH/FC/CHC/FM leads (Databricks also includes HFC/GH leads). The edidatasourcefk value is retrieved from the lookup table based on matching source combinations. NO hardcoded values are used for globalmrn_assign pipeline. | Source: mappededidatasourcefk lookup table (Delta Lake Parquet ADLS) - edidatasourcefk field. Processing: Join based on leadsource/xrefsource/associationsource combination for ICH/FC/CHC/FM/HFC leads. The edidatasourcefk value is retrieved from the lookup table based on matching source combinations. NO hardcoded values are used for globalmrn_assign pipeline. Databricks includes additional HFC source type (lsb_xrefsourcevalue='GH') not present in Hadoop. | Similar | Both use lookup tables (not hardcoded values). Databricks includes additional HFC source type. Storage differs: MapR DB (Hadoop) vs Parquet (Databricks).
```

---

## 2. Other STTM Differences Found

### Difference in bestleadselection PARTITION BY clause:

**Hadoop (`process_leads.py:535`):**
```sql
ROW_NUMBER() OVER (PARTITION BY patientacctifk,lsb_edipartnerfk,lsb_coverageid,lsb_id 
ORDER BY CAST(lsb_demoupdatedate AS TIMESTAMP) DESC,demohash DESC) AS rn
```

**Databricks (`process_leads.py:349`):**
```sql
ROW_NUMBER() OVER (PARTITION BY patientacctifk,hospitalfk,lsb_edipartnerfk,lsb_coverageid,lsb_id 
ORDER BY CAST(lsb_demoupdatedate AS TIMESTAMP) DESC,demohash DESC, lsb_leadsourcevalue asc, lsb_xrefsourcevalue asc) AS rn
```

**Differences:**
1. Databricks includes `hospitalfk` in PARTITION BY (Hadoop does not)
2. Databricks includes `lsb_leadsourcevalue asc, lsb_xrefsourcevalue asc` in ORDER BY (Hadoop does not)

**Second ROW_NUMBER window:**

**Hadoop (`process_leads.py:578`):**
```sql
ROW_NUMBER() OVER (PARTITION BY patientacctifk,lsb_edipartnerfk,lsb_coverageid,rn 
ORDER BY CAST(keyvalue AS INT) ASC, edidatasourcetablekey) AS rn2
```

**Databricks (`process_leads.py:393`):**
```sql
ROW_NUMBER() OVER (PARTITION BY patientacctifk,hospitalfk,lsb_edipartnerfk,upper(trim(lsb_coverageid)),rn 
ORDER BY CAST(keyvalue AS INT) ASC, edidatasourcetablekey) AS rn2
```

**Differences:**
1. Databricks includes `hospitalfk` in PARTITION BY
2. Databricks uses `upper(trim(lsb_coverageid))` instead of `lsb_coverageid`

**Impact:** These differences may result in different deduplication logic and could affect which lead is selected when multiple leads exist for the same patient account.

---

## 3. Flow Diagrams

### Hadoop Workflow Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    START - Oozie Workflow                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Check Previous  │
                    │  WF Status      │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Restart Failed  │
                    │      WF         │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Get Notification│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Check           │
                    │ Notification    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   Get Date      │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ 360 Logger      │
                    │   (Running)     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Get MinMax DT   │
                    │  (Admit Dates)  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Get Candidate   │
                    │  Patient Accts  │
                    │  (3 days filter)│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Process CPA     │
                    │  LSB XTable     │
                    └────────┬────────┘
                             │
                             ▼
        ┌────────────────────┴────────────────────┐
        │                                          │
        ▼                                          ▼
┌───────────────┐                          ┌───────────────┐
│ Process      │                          │ Process       │
│ GlobalMRN    │                          │ PermID        │
│ LeadLookup   │                          │ LeadLookup    │
└───────┬───────┘                          └───────┬───────┘
        │                                          │
        └──────────────────┬───────────────────────┘
                           │
                           ▼
        ┌──────────────────┴──────────────────┐
        │                                      │
        ▼                                      ▼
┌───────────────┐                    ┌───────────────┐
│ Process SSN  │                    │ Process       │
│ LeadLookup   │                    │ MedicalRecNum │
└───────┬───────┘                    │ LeadLookup    │
        │                            └───────┬───────┘
        │                                      │
        └──────────────┬───────────────────────┘
                       │
                       ▼
              ┌───────────────┐
              │ Process       │
              │ ClusteredAcct │
              │ LeadLookup    │
              └───────┬───────┘
                       │
                       ▼
              ┌───────────────┐
              │ Merge CPA     │
              │ X Leads       │
              └───────┬───────┘
                       │
                       ▼
              ┌───────────────┐
              │ Process Leads │
              │ (Filter Chain)│
              └───────┬───────┘
                       │
                       ▼
              ┌───────────────┐
              │ HDFS Dir      │
              │    Check      │
              └───────┬───────┘
                       │
                       ▼
              ┌───────────────┐
              │ Check Leads   │
              └───────┬───────┘
                       │
                       ▼
              ┌───────────────┐
              │ Sqoop Out     │
              │ (to DB)       │
              └───────┬───────┘
                       │
                       ▼
              ┌───────────────┐
              │ Update        │
              │ Notification  │
              └───────┬───────┘
                       │
                       ▼
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
┌───────────────┐            ┌───────────────┐
│ Log           │            │ Email Notify  │
│ Notification  │            │   Success    │
└───────┬───────┘            └───────┬───────┘
        │                             │
        └──────────────┬──────────────┘
                       │
                       ▼
              ┌───────────────┐
              │ Purge         │
              │ Intermediate  │
              │     Data      │
              └───────┬───────┘
                       │
                       ▼
              ┌───────────────┐
              │ 360 Logger    │
              │  (Finished)   │
              └───────┬───────┘
                       │
                       ▼
              ┌───────────────┐
              │      END      │
              └───────────────┘
```

### Databricks Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    START - ADF Pipeline                           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Set Workflow    │
                    │     Type        │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Set             │
                    │ Notification    │
                    │     Type        │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Get Breadcrumb  │
                    │   (from DB)     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Set Breadcrumb  │
                    │   Variable      │
                    └────────┬────────┘
                             │
                             ▼
        ┌────────────────────┴────────────────────┐
        │                                          │
        ▼                                          ▼
┌───────────────┐                          ┌───────────────┐
│ Update       │                          │ 360 Logger    │
│ Notification │                          │   (Running)   │
│ (In Progress)│                          └───────┬───────┘
└───────┬───────┘                                  │
        │                                          │
        └──────────────────┬───────────────────────┘
                           │
                           ▼
                    ┌─────────────────┐
                    │ Get MinMax DT   │
                    │  (Admit Dates)  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Get Candidate   │
                    │  Patient Accts  │
                    │ (90 days filter)│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ LSB Lookup      │
                    │  (Optimized)    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Process Leads   │
                    │ (Filter Chain)  │
                    │                 │
                    │ 1. Extract &    │
                    │    Explode      │
                    │ 2. Blacklist    │
                    │ 3. Coverage ID  │
                    │    Length       │
                    │ 4. Commercial/  │
                    │    Tricare      │
                    │ 5. HPN Enabled  │
                    │ 6. Min Charges  │
                    │ 7. BCBS Prefix   │
                    │ 8. Filter FC    │
                    │ 9. Filter FC    │
                    │    BCBS         │
                    │ 10. Walling Off │
                    │ 11. EDI Data    │
                    │     Source FK   │
                    │ 12. OHI Process │
                    │ 13. Billing     │
                    │     Deadline    │
                    │ 14. Known      │
                    │     Coverages   │
                    │ 15. Best Lead  │
                    │     Selection   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Check Output    │
                    │    Exists?      │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │                 │
                    ▼                 ▼
            ┌───────────┐     ┌───────────────┐
            │   Yes     │     │      No       │
            │           │     │               │
            │ Check     │     │ Set Return   │
            │  Leads    │     │ Value = 0     │
            └─────┬─────┘     └───────┬───────┘
                  │                   │
                  └─────────┬─────────┘
                            │
                            ▼
                    ┌─────────────────┐
                    │ Check Leads     │
                    │   Exist?        │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │                 │
                    ▼                 ▼
            ┌───────────┐     ┌───────────────┐
            │   Yes     │     │      No       │
            │           │     │               │
            │ Sqoop Out │     │ Skip Sqoop    │
            │ (to DB)   │     │               │
            └─────┬─────┘     └───────┬───────┘
                  │                   │
                  └─────────┬─────────┘
                            │
                            ▼
                    ┌─────────────────┐
                    │ Delete Trigger  │
                    │     File        │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Update          │
                    │ Notification    │
                    │  (Completed)    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      END        │
                    └─────────────────┘
```

---

## Key Differences in Flow:

1. **Hadoop**: Uses parallel lookups (fork/join) for multiple identity types (GlobalMRN, PermID, SSN, MedicalRecNum, ClusteredAcct)
2. **Databricks**: Uses optimized LSB lookup (single step) instead of multiple parallel lookups
3. **Hadoop**: Has explicit merge step after lookups
4. **Databricks**: Has additional filters (BCBS prefix, billing deadline, known coverages) not in Hadoop
5. **Hadoop**: Uses 3 days for vsnapflags filter
6. **Databricks**: Uses 90 days for vsnapflags filter
7. **Hadoop**: Has explicit purge step
8. **Databricks**: Notification update happens at end

