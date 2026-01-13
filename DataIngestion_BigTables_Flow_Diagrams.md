# Data Ingestion Big Tables Flow Diagrams

## Hadoop Workflow 1: escan_data_ingestion : big_tables

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Oozie WF Starter Check      │
│ (Shell script - check if    │
│  workflow can start)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Can Start?         │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get DateTime│    │ End         │
│ (Shell)     │    └─────────────┘
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Fork: Ingest Tables          │
│ Group 1 (Parallel)           │
│ (5 big tables)               │
└───┬──────────────────────┬───┘
    │                      │
    ├──────────────────────┼──────────────────────────────────────────────┐
    │                      │                                              │
    ▼                      ▼                                              ▼
┌─────────────┐    ┌─────────────┐                              ┌──────────────────┐
│ Process     │    │ Process     │    ... (3 more tables)       │ Process          │
│ VendorKnown │    │ EDI Query   │                              │ Hospital Import  │
│ OHI         │    │ Hits and    │                              │ Payment          │
│ Coverages   │    │ Misses      │                              │                  │
│ (Sub-workflow│    │ (Sub-workflow│                              │ (Sub-workflow)   │
│  sqoop_     │    │  sqoop_     │                              │                  │
│  workflow)  │    │  workflow)  │                              │                  │
└──────┬──────┘    └──────┬──────┘                              └──────┬───────────┘
       │                  │                                             │
       └──────────────────┴─────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────┐
│ Join: End Group 1            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Hadoop Workflow 2: escan_data_ingestion : sqoop_table

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check Previous               │
│ Workflow Status             │
│ (Spark - MapR DB)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Restart Previous            │
│ Failed Workflow             │
│ (Shell script)              │
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
│ Log Workflow│    │ End         │
│ Start       │    └─────────────┘
│ (Spark -    │
│  MapR DB)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Read Date for Sqoop          │
│ (Shell - get last value      │
│  from previous run)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Table Type?        │
└───┬────────────────────┬────┘
    │ Demographics       │ Hospital Import Payment
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Delta │    │ Spark JDBC  │
│ Demographics│    │ Extract     │
│ (Sqoop)     │    │ Incremental │
│             │    │ (Spark)     │
└──────┬──────┘    └──────┬──────┘
       │                 │
       │                 │
       │    Default      │
       └──────────┬──────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Sqoop Delta                  │
│ (Sqoop - incremental extract │
│  based on last value)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Publish Delta                │
│ (Shell - publish data to     │
│  served location)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Save Max Date for Sqoop      │
│ (Spark - save last value     │
│  for next run)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Clean Up Delta Input         │
│ (Shell - move input data     │
│  to recycle bin)             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Log Workflow Finish          │
│ (Spark - MapR DB)           │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ End              │    │ Log Workflow    │
│                  │    │ Fail            │
│                  │    │ (Error path)     │
└──────────────────┘    └──────┬──────────┘
                                │
                                ▼
                         ┌─────────────┐
                         │ End         │
                         └─────────────┘
```

## Databricks Pipeline: pl_dataingestion_big_tables

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get BC                      │
│ (Get breadcrumb date        │
│  from Databricks notebook)  │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set BC Variable  │  │ Set BC Return    │
│                  │  │ (Pipeline return │
│                  │  │  value)          │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       │                     │
       ├─────────────────────┤
       │                     │
       ▼                     ▼
┌──────────────────┐  ┌──────────────────┐
│ Log Workflow     │  │ Update           │
│ Start            │  │ Notification     │
│ (Cosmos DB)      │  │ Running          │
│                  │  │ (Cosmos DB)      │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Fork: Process Big Tables     │
│ (Parallel - 5+ tables)       │
└───┬──────────────────────┬───┘
    │                      │
    ├──────────────────────┼──────────────────────────────────────────────┐
    │                      │                                              │
    ▼                      ▼                                              ▼
┌─────────────┐    ┌─────────────┐                              ┌──────────────────┐
│ Hospital    │    │ Vendor Known│    ... (3+ more tables)       │ Demographics     │
│ Import      │    │ OHI         │                              │                  │
│ Payment     │    │ Coverages   │                              │                  │
│ (Extract)   │    │ (Extract)   │                              │ (Extract)        │
└──────┬──────┘    └──────┬──────┘                              └──────┬───────────┘
       │                 │                                             │
       ▼                 ▼                                             ▼
┌─────────────┐    ┌─────────────┐                              ┌──────────────────┐
│ Publish     │    │ Publish     │                              │ Publish          │
│ Hospital    │    │ Vendor Known│                              │ Demographics     │
│ Import      │    │ OHI         │                              │                  │
│ Payment     │    │ Coverages   │                              │                  │
└──────┬──────┘    └──────┬──────┘                              └──────┬───────────┘
       │                 │                                             │
       └──────────────────┴─────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────┐
│ Update Notification          │
│ Processed                    │
│ (Update Cosmos DB           │
│  runstatus to processed)    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Key Process Steps

### Hadoop Workflow 1: escan_data_ingestion : big_tables

1. **Oozie WF Starter Check** - Shell script to check if workflow can start (checks lock file)
2. **Decision: Can Start?** - If check returns true, continue; else end
3. **Get DateTime** - Shell script to extract breadcrumb date
4. **Fork: Ingest Tables Group 1** - Parallel execution of 5 big table ingestion sub-workflows:
   - vendorknownohicoverages
   - ediqueryhitsandmisses
   - ediqueries
   - demographics
   - hospitalimportpayment
5. **Join: End Group 1** - Wait for all parallel table ingestions to complete
6. **End** - Workflow completion

### Hadoop Workflow 2: escan_data_ingestion : sqoop_table

1. **Check Previous Workflow Status** - Spark job to check MapR DB for failed workflows
2. **Restart Previous Failed Workflow** - Shell script to restart failed workflows
3. **Decision: Restart Successful?** - If restart successful, continue; else end
4. **Log Workflow Start** - Spark job to log RUNNING status to MapR DB
5. **Read Date for Sqoop** - Shell script to get last value from previous run
6. **Decision: Table Type?** - Route to appropriate extraction method:
   - Demographics: Special Sqoop query with selected columns
   - Hospital Import Payment: Spark JDBC incremental extract
   - Default: Standard Sqoop delta extract
7. **Sqoop Delta / Spark JDBC Extract** - Extract incremental data based on last value
8. **Publish Delta** - Shell script to publish data to served location
9. **Save Max Date for Sqoop** - Spark job to save last value for next run
10. **Clean Up Delta Input** - Shell script to move input data to recycle bin
11. **Log Workflow Finish** - Spark job to log FINISHED status to MapR DB
12. **End or Log Fail** - End workflow or log failure

### Databricks Pipeline: pl_dataingestion_big_tables

1. **Get BC** - Databricks notebook (get_bc) to get breadcrumb date
2. **Set BC Variable** - Set breadcrumb variable from get_bc output
3. **Set BC Return** - Set pipeline return value with breadcrumb (parallel with Set BC Variable)
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360 (parallel with update notification)
5. **Update Notification Running** - Update Cosmos DB runstatus table to "running" (parallel with log workflow start)
6. **Fork: Process Big Tables** - Parallel execution of table extraction activities (5+ tables):
   - hospitalimportpayment
   - vendorknownohicoverages
   - ediqueryhitsandmisses
   - ediqueries
   - demographics
   - (and potentially more)
7. **Publish Activities** - Each table has a corresponding publish activity after extraction
8. **Update Notification Processed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Two separate workflows - main workflow (big_tables) orchestrates 5 sub-workflows (sqoop_table)
- **Databricks**: Single pipeline that handles all big table extractions with parallel activities
- **Hadoop Workflow 1**: Uses fork/join to run 5 parallel sub-workflows
- **Hadoop Workflow 2**: Individual sub-workflow that handles incremental extraction with delta logic, checks previous workflow status, restarts failed workflows, reads last value, extracts delta, publishes, saves max date, and cleans up
- **Databricks**: Uses parallel activities for each table extraction, each followed by a publish activity
- **Hadoop**: Uses Sqoop actions for SQL Server data extraction (with special handling for demographics and hospitalimportpayment)
- **Databricks**: Uses Databricks notebook (sqoop_delta) with JDBC connections for SQL Server data extraction
- **Hadoop**: Uses Spark JDBC for hospitalimportpayment incremental extraction
- **Databricks**: Uses same notebook approach for all tables
- **Hadoop**: Uses shell scripts for publishing data (publish_delta_data.sh)
- **Databricks**: Uses Databricks notebook (publish_sqoop_data) for publishing
- **Hadoop**: Saves last value using Spark job (fetch_last_value.py) for next incremental run
- **Databricks**: Last value tracking handled within extract notebook
- **Hadoop**: Cleans up input data by moving to recycle bin
- **Databricks**: Cleanup handled within notebooks
- **Hadoop**: Uses shell scripts for workflow starter check and datetime extraction
- **Databricks**: Uses Databricks notebooks for all operations
- **Hadoop**: Email notifications not shown in workflow (may be in coordinator)
- **Databricks**: No email notifications, uses Cosmos DB for logging and notifications
- **Hadoop**: Uses HDFS for data storage
- **Databricks**: Uses ADLS (Azure Data Lake Storage) for data storage
- **Hadoop**: Uses MapR filesystem (maprfs://) and MapR DB for logging
- **Databricks**: Uses Azure storage (abfss://) and Cosmos DB for logging
- **Hadoop**: Workflow starter check with lock file mechanism
- **Databricks**: No workflow starter check, uses Cosmos DB notifications
- **Hadoop**: Separate sub-workflow for each table with full delta logic
- **Databricks**: Each table has extract and publish activities, delta logic in notebooks
- **Hadoop**: Includes restart logic for failed workflows
- **Databricks**: Retry logic at activity level (retry: 2, retryIntervalInSeconds: 2700)
- **Hadoop**: Special handling for demographics (selected columns only) and hospitalimportpayment (Spark JDBC)
- **Databricks**: Uniform handling for all tables through same notebook with parameters
