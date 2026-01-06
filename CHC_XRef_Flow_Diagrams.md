# CHC XRef Flow Diagrams

## Hadoop Workflow: escan_data_ingestion : CHC_PERM_GMRN_ID_XREF

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check Previous Failed       │
│ Workflow Status             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Restart Previous            │
│ Failed Workflow             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Restart            │
│ Successful?                 │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get Date    │    │ End         │
│ (Breadcrumb)│    └─────────────┘
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Check XRef PermID            │
│ (Check MapR DB for permid   │
│  notification)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Read XRef PermID            │
│ (Read workflow ID and       │
│  breadcrumb from log)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Rows Found?       │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Log Start   │    │ End         │
└──────┬──────┘    └─────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Parse BCS File              │
│ Fixed-Width                  │
│ (Convert fixed-width to     │
│  parquet using Pig)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ XRef Update PermID          │
│ (Update PermID cross-ref    │
│  in publish and xref tables)│
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notification Perm XRef      │
│ (Update MapR DB perm_xref)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check XRef PermID Upd       │
│ (Check for updated permid    │
│  notification)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Read XRef PermID Upd        │
│ (Read workflow ID and       │
│  breadcrumb)                │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Rows Found?       │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ GMRN        │    │ Email Fail  │
│ Validated   │    └─────────────┘
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ CHC Validated                │
│ (Validate CHC data with     │
│  PermID index)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge SSN GlobalMRN         │
│ (Match on SSN)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge PID GlobalMRN          │
│ (Match on Policy ID)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge SimSSN GlobalMRN       │
│ (Match on similar SSN)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge AntiSSN GlobalMRN     │
│ (Match on anti-similar SSN) │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge Soundex GlobalMRN     │
│ Part 01, 02, 03             │
│ (Match on Soundex name)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge PolicyID GlobalMRN    │
│ (Match on Policy ID)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Graph Data            │
│ (Create graph vertices and  │
│  edges)                      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Graph Connections     │
│ (Find connected components) │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Group GlobalMRN              │
│ (Process graph data, assign │
│  GlobalMRN groups)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge Decisions              │
│ (Combine all match decisions)│
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ XRef Update GMRNID           │
│ (Update GMRNID cross-ref    │
│  in transaction and xref)   │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Notification     │  │ Notification     │
│ GMRN RX          │  │ GMRN TX          │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Notification GMRN XRef       │
│ (Update MapR DB gmrn_xref)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notify LSB CHC               │
│ (Create notification for     │
│  Lead Service Base)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Spark JDBC Push              │
│ (Push data to SQL Server     │
│  via JDBC)                   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Fork: Parallel Execution    │
└───┬────────────────────┬────┘
    │                    │
    ▼                    ▼
┌─────────────┐  ┌─────────────┐
│ Log Finish  │  │ Email Notify│
└──────┬──────┘  └──────┬──────┘
       │                │
       └────────┬───────┘
                │
                ▼
        ┌───────────────┐
        │ Janitor       │
        │ (Cleanup)     │
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │ End           │
        └───────────────┘
```

## Databricks Pipeline: pl_chc_xref

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Breadcrumb              │
│ (Query Cosmos DB for        │
│  unprocessed notifications) │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set Breadcrumb   │  │ Breadcrumb      │
│ Variable         │  │ Pipeline Return │
└──────┬───────────┘  └──────────────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Log Workflow     │  │ Update          │
│ Start            │  │ Notification    │
│ (Cosmos DB)      │  │ In Progress     │
└──────┬───────────┘  └──────┬──────────┘
       │                     │
       └──────────┬───────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Set Basepath                 │
│ Variable                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notification Engine          │
│ (Update Cosmos DB            │
│  bcsdownload = '1')          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Parse BCS File Fixed         │
│ (Convert fixed-width to      │
│  parquet using Spark)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ XRef PermID                  │
│ (Update PermID cross-ref     │
│  in publish and xref tables) │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notification Engine          │
│ PermID                       │
│ (Update Cosmos DB            │
│  permid = '1')               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ GMRN Validated                │
│ (Validate GMRN data)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ CHC Validated                 │
│ (Validate CHC data with      │
│  PermID index)                │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Wait1                        │
│ (Synchronization point)     │
└──────────┬──────────────────┘
           │
           ├──────────────────────────────────────────────┐
           │                                              │
           ▼                                              ▼
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│ Merge SSN            │  │ Merge PID            │  │ Merge SimSSN         │
│ GlobalMRN            │  │ GlobalMRN             │  │ GlobalMRN            │
│ (Parallel)           │  │ (Parallel)            │  │ (Parallel)           │
└──────┬───────────────┘  └──────┬───────────────┘  └──────┬───────────────┘
       │                        │                        │
       └────────────────────────┼────────────────────────┘
                                │
                                ▼
┌─────────────────────────────┐
│ Merge AntiSSN GlobalMRN     │
│ (Parallel)                   │
└──────────┬──────────────────┘
           │
           ├──────────────────────────────────────────────┐
           │                                              │
           ▼                                              ▼
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│ Merge Soundex        │  │ Merge Soundex        │  │ Merge Soundex        │
│ GlobalMRN Part 01    │  │ GlobalMRN Part 02   │  │ GlobalMRN Part 03   │
│ (Parallel)            │  │ (Parallel)           │  │ (Parallel)           │
└──────┬───────────────┘  └──────┬───────────────┘  └──────┬───────────────┘
       │                        │                        │
       └────────────────────────┼────────────────────────┘
                                │
                                ▼
┌─────────────────────────────┐
│ Merge PolicyID GlobalMRN    │
│ (Parallel)                   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Wait2                        │
│ (Synchronization point)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge Phonetics GlobalMRN   │
│ (Optimized merge)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Graph Data            │
│ (Create graph vertices and  │
│  edges)                      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Graph Connections     │
│ (Find connected components) │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Process Graph Data           │
│ (Assign GlobalMRN groups)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge Decisions              │
│ (Combine all match decisions)│
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ XRef GMRNID                  │
│ (Update GMRNID cross-ref     │
│  in transaction and xref)    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Delete Staging               │
│ (Clean up staging tables)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ JDBC Push                    │
│ (Push data to SQL Server     │
│  via JDBC)                   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notification Engine          │
│ GMRNID                       │
│ (Update Cosmos DB            │
│  gmrnid = '1')               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Log Notification             │
│ LSB CHC                      │
│ (Create notification for     │
│  Lead Service Base)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Trigger File          │
│ (Create trigger for          │
│  downstream processing)      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Delete Trigger File          │
│ (Delete original trigger)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Notification          │
│ Processed                    │
│ (Update Cosmos DB            │
│  processed = 'processed')    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Key Process Steps

### Hadoop Workflow Steps:

1. **Check Previous Failed Workflow Status** - Check MapR DB for failed workflows
2. **Restart Previous Failed Workflow** - Attempt to restart failed workflows
3. **Get Date** - Extract breadcrumb (date) from parameters
4. **Check XRef PermID** - Check MapR DB for PermID notification status
5. **Read XRef PermID** - Read workflow ID and breadcrumb from log file
6. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
7. **Parse BCS File Fixed-Width** - Convert fixed-width BCS response file to parquet using Pig script
8. **XRef Update PermID** - Update PermID cross-reference in publish and xref tables
9. **Notification Perm XRef** - Update MapR DB notification with perm_xref flag
10. **Check XRef PermID Upd** - Check MapR DB for updated PermID notification
11. **GMRN Validated** - Validate GMRN demographics data
12. **CHC Validated** - Validate CHC data with PermID index
13. **Merge SSN GlobalMRN** - Match records on SSN
14. **Merge PID GlobalMRN** - Match records on Policy ID
15. **Merge SimSSN GlobalMRN** - Match records on similar SSN
16. **Merge AntiSSN GlobalMRN** - Match records on anti-similar SSN
17. **Merge Soundex GlobalMRN** (Part 01, 02, 03) - Match records on Soundex name algorithm
18. **Merge PolicyID GlobalMRN** - Match records on Policy ID
19. **Create Graph Data** - Create graph vertices and edges for relationship mapping
20. **Create Graph Connections** - Find connected components using graph algorithm
21. **Group GlobalMRN** - Process graph data and assign GlobalMRN groups
22. **Merge Decisions** - Combine all match decisions into final output
23. **XRef Update GMRNID** - Update GMRNID cross-reference in transaction and xref tables
24. **Notifications** (GMRN RX, TX, XRef) - Update MapR DB notifications
25. **Notify LSB CHC** - Create notification for Lead Service Base
26. **Spark JDBC Push** - Push data to SQL Server via JDBC
27. **Log Workflow Finish** - Log FINISHED status to MapR DB
28. **Janitor** - Clean up temporary files
29. **Email Notify** - Send success email

### Databricks Pipeline Steps:

1. **Get Breadcrumb** - Query Cosmos DB runstatus table for unprocessed notifications (chc_xref)
2. **Set Breadcrumb Variable** - Extract and set breadcrumb from notification
3. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360
4. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running"
5. **Set Basepath Variable** - Set base path for data processing
6. **Notification Engine** - Update Cosmos DB chc_tracker with bcsdownload = '1'
7. **Parse BCS File Fixed** - Convert fixed-width BCS response file to parquet using Spark
8. **XRef PermID** - Update PermID cross-reference in publish and xref tables
9. **Notification Engine PermID** - Update Cosmos DB chc_tracker with permid = '1'
10. **GMRN Validated** - Validate GMRN demographics data
11. **CHC Validated** - Validate CHC data with PermID index
12. **Wait1** - Synchronization point
13. **Parallel Merge Operations** - Merge SSN, PID, SimSSN, AntiSSN, Soundex (parts 01-03), PolicyID GlobalMRN (executed in parallel)
14. **Wait2** - Synchronization point after parallel merges
15. **Merge Phonetics GlobalMRN** - Optimized phonetics merge
16. **Create Graph Data** - Create graph vertices and edges
17. **Create Graph Connections** - Find connected components
18. **Process Graph Data** - Assign GlobalMRN groups
19. **Merge Decisions** - Combine all match decisions
20. **XRef GMRNID** - Update GMRNID cross-reference in transaction and xref tables
21. **Delete Staging** - Clean up staging tables
22. **JDBC Push** - Push data to SQL Server via JDBC
23. **Notification Engine GMRNID** - Update Cosmos DB chc_tracker with gmrnid = '1'
24. **Log Notification LSB** - Create notification for Lead Service Base
25. **Create Trigger File** - Create trigger file for downstream processing
26. **Delete Trigger File** - Delete original trigger file
27. **Update Notification Processed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Sequential execution of merge operations
- **Databricks**: Parallel execution of merge operations (SSN, PID, SimSSN, AntiSSN, Soundex parts, PolicyID) with Wait synchronization points
- **Hadoop**: Uses Pig script for fixed-width to parquet conversion
- **Databricks**: Uses Spark for fixed-width to parquet conversion
- **Hadoop**: Includes janitor step for cleanup
- **Databricks**: Includes delete staging step for cleanup
- **Hadoop**: Uses MapR DB for notifications
- **Databricks**: Uses Cosmos DB for notifications
- **Databricks**: Includes optimized Merge Phonetics step (not in Hadoop workflow)
- **Hadoop**: Separate notifications for GMRN RX, TX, and XRef
- **Databricks**: Single notification engine with different column names (bcsdownload, permid, gmrnid)

