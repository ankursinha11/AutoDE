1. LeadLookup (Commercial) - Hadoop vs Databricks

### Hadoop: leaddiscovery: leadlookup:
```
START
  ↓
1. Check Previous WF Status
  ↓
2. Restart Previous Failed WF (if needed)
  ↓
3. Get Notification (from MapR DB)
  ↓
4. Check Notification
  ↓
5. Get Date/Time
  ↓
6. Oozie 360 Log Start
  ↓
7. Get Min/Max Admit Dates (Create Lookup)
  ↓
8. Get Candidate Patient Accounts
  ↓
9. Process CPA-LSB Cross-Table
  ↓
10. FORK: Parallel Lead Lookups (Group 1)
    ├─→ Process GlobalMRN LeadLookup (Java JAR)
    └─→ Process PermId LeadLookup (Java JAR)
  ↓
11. FORK: Parallel Lead Lookups (Group 2)
    ├─→ Process SSN LeadLookup (Java JAR)
    ├─→ Process MedicalRecNum LeadLookup (Java JAR)
    └─→ Process ClusteredAcctFK LeadLookup (Java JAR)
  ↓
12. Merge CPA x Leads
  ↓
13. Process Leads
  ↓
14. HDFS Dir Check
  ↓
15. Check Leads (if data exists)
  ↓
16. HDFS Dir Check Leads
  ↓
17. Sqoop Out (to SQL Server)
  ↓
18. Sqoop Out HDPBatch
  ↓
19. Update Notification
  ↓
20. FORK: Final Steps
    ├─→ Log Notification
    ├─→ Email Notify Success
    ├─→ Purge Intermediate Data
    └─→ Oozie 360 Log Finish
  ↓
END
```

### Databricks: pl_leaddiscovery_globalmrn_assign
```
START
  ↓
1. Set Workflow Type (regular)
  ↓
2. Set Notification Type (globalmrn_assign)
  ↓
3. Get Breadcrumb (from Cosmos DB)
  ↓
4. Set Breadcrumb
  ↓
5. FORK: Initial Setup
    ├─→ 360 Logger Running
    └─→ Update Notification InProgress
  ↓
6. Get Min/Max Admit Dates (Create Lookup)
  ↓
7. Get Candidate Patient Accounts
  ↓
8. LSB Lookup (Optimized - all identity types)
  ↓
9. Process Leads
  ↓
10. Check If Process Lead Output Exists
    ├─→ YES: Check Leads
    │     ↓
    │   Check If Leads Exist
    │     ├─→ YES: Sqoop Out (to SQL Server)
    │     └─→ NO: Skip
    └─→ NO: Skip
  ↓
11. Delete Trigger File
  ↓
12. Update Notification Completed
  ↓
END
```

---

## 2. ESCAN Import FC - Hadoop vs Databricks

### Hadoop: leadrepository: escan_import_fc
```
START
  ↓
1. Check Previous WF Status
  ↓
2. Restart Previous Failed WF (if needed)
  ↓
3. Get Notification (from MapR DB)
  ↓
4. Check Notification
  ↓
5. Get Date/Time
  ↓
6. Oozie 360 Log Start
  ↓
7. Parse FC Transaction Demo (Main Processing)
    - Read FoundCoverage data
    - Join with EDIQueries, HitStatus
    - Filter and transform
    - Write to lr_transaction (cooked)
    - Write to toserve path
  ↓
8. Create Notification FC XRef
  ↓
9. FORK: Final Steps
    ├─→ Oozie 360 Log Finish
    ├─→ Log Notification
    ├─→ Update Notification
    └─→ Email Notify Success
  ↓
END
```

### Databricks: pl_leadrepo_escan_import_fc
```
START
  ↓
1. Lookup for trange and fc_source_key (from config JSON)
  ↓
2. FORK: Set Variables
    ├─→ Set trange
    └─→ Set fc_source_key
  ↓
3. Get Breadcrumb (from Cosmos DB)
  ↓
4. Set Breadcrumb
  ↓
5. FORK: Initial Setup
    ├─→ 360 Logger Running
    └─→ Update Notification InProgress
  ↓
6. Reconcile LeadRepo Transaction Demo (Main Processing)
    - Read FoundCoverage data
    - Join with EDIQueries, HitStatus
    - Filter and transform
    - Write to lr_transaction (cooked)
    - Write to toserve path
  ↓
7. Log Notification LeadRepo FC XRef
  ↓
8. Create Trigger File FC XRef
  ↓
9. Delete Trigger File
  ↓
10. Update Notification Completed
  ↓
END
```
