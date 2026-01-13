# Data Ingestion FNF Flow Diagrams

## Hadoop Workflow 1: escan_data_ingestion : sqoop_fnf

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
│ Decision: Can Start?        │
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
│ Group 1 (Parallel)          │
│ (19 tables + 1 Spark JDBC)  │
└───┬──────────────────────┬───┘
    │                      │
    ├──────────────────────┼──────────────────────────────────────────────┐
    │                      │                                              │
    ▼                      ▼                                              ▼
┌─────────────┐    ┌─────────────┐                              ┌──────────────────┐
│ Sqoop FNF   │    │ Sqoop FNF   │    ... (17 more tables)     │ Spark JDBC FNF   │
│ Table 1     │    │ Table 2     │                              │ Series           │
│ (Sub-workflow│    │ (Sub-workflow│                              │ (Sub-workflow)   │
│  lsb_conf_  │    │  lsb_conf_  │                              │                  │
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
│ Decision: Email?             │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Email Notify│    │ Email Notify│
│ Success     │    │ Success     │
│             │    │             │
└──────┬──────┘    └──────┬──────┘
       │                 │
       └──────────┬───────┘
                  │
                  ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Hadoop Workflow 2: escan_data_ingestion : lsb_conf_tables_fnf

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ LSB Sqoop Table FNF          │
│ (Sqoop import - extract     │
│  table data from SQL Server) │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ LSB Publish      │    │ Fail            │
│ Sqoop Data       │    │ (Error path)     │
│ (Spark - publish  │    │                  │
│  data to served  │    │                  │
│  location)       │    │                  │
└──────┬───────────┘    └──────┬───────────┘
       │                      │
       └──────────┬───────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Databricks Pipeline: pl_dataingestion_fnf

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
│ Wait                        │
│ (Wait 1 second)             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Parallel Extract FNF         │
│ (Extract all FNF tables      │
│  in parallel using           │
│  Databricks notebook)        │
└──────────┬──────────────────┘
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

### Hadoop Workflow 1: escan_data_ingestion : sqoop_fnf

1. **Oozie WF Starter Check** - Shell script to check if workflow can start (checks lock file)
2. **Decision: Can Start?** - If check returns true, continue; else end
3. **Get DateTime** - Shell script to extract breadcrumb date
4. **Fork: Ingest Tables Group 1** - Parallel execution of 19 table ingestion sub-workflows plus 1 Spark JDBC sub-workflow:
   - partnerpolicyidblacklists
   - zipcodes
   - edi270querycombos
   - edisubscriberdobresponse
   - hpdhospitalpartnerlastrespstatus
   - edipartnerattributes
   - edipartnerattributevalues
   - hospitalnpioverrides
   - hospitalpayercob
   - edidatasources
   - edileadsourcetransition
   - escanglobalparameters
   - edipartnertype
   - edisubmitters
   - edipartnersubmittersoverrides
   - edipartner270settings
   - edipartnerhospital270settingsoverrides
   - foundcoverageinferredsourcemethodcodes
   - edipayers_tupayeridmapping
   - spark_fnf_jdbc_series (sub-workflow for Spark JDBC extraction)
5. **Join: End Group 1** - Wait for all parallel table ingestions to complete
6. **Decision: Email?** - Check if email should be sent
7. **Email Notify Success** - Send success email notification
8. **End** - Workflow completion

### Hadoop Workflow 2: escan_data_ingestion : lsb_conf_tables_fnf

1. **LSB Sqoop Table FNF** - Sqoop action to import table data from SQL Server to HDFS input location
2. **LSB Publish Sqoop Data** - Spark job (lsb_publish_sqoop_data.py) to publish sqooped data from input to served location (current/20991231)
3. **End** - Sub-workflow completion (used by main workflow)

### Databricks Pipeline: pl_dataingestion_fnf

1. **Get BC** - Databricks notebook (get_bc) to get breadcrumb date in YMDTH format
2. **Set BC Variable** - Set breadcrumb variable from get_bc output
3. **Set BC Return** - Set pipeline return value with breadcrumb (parallel with Set BC Variable)
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360 (parallel with update notification)
5. **Update Notification Running** - Update Cosmos DB runstatus table to "running" for dataingestion_fnf_scheduletrigger (parallel with log workflow start)
6. **Wait** - Wait activity (1 second) after logging and notification updates complete
7. **Parallel Extract FNF** - Databricks notebook (dataingestion_extract_fnf) to extract all FNF tables in parallel from SQL Server to ADLS
8. **Update Notification Processed** - Update Cosmos DB runstatus table to "processed" for dataingestion_fnf_scheduletrigger

## Key Differences

- **Hadoop**: Two separate workflows - main workflow (sqoop_fnf) orchestrates 19 sub-workflows (lsb_conf_tables_fnf) plus 1 Spark JDBC sub-workflow
- **Databricks**: Single pipeline that handles all table extractions in one notebook
- **Hadoop Workflow 1**: Uses fork/join to run 20 parallel sub-workflows (19 Sqoop + 1 Spark JDBC)
- **Hadoop Workflow 2**: Individual sub-workflow that runs Sqoop import and Spark publish for each table
- **Databricks**: Uses single notebook (dataingestion_extract_fnf) that handles all table extractions internally with parallel processing
- **Hadoop**: Uses Sqoop actions for SQL Server data extraction
- **Databricks**: Uses Databricks notebook with JDBC connections for SQL Server data extraction
- **Hadoop**: Uses Spark job (lsb_publish_sqoop_data.py) for publishing data
- **Databricks**: Publishing logic included in extract notebook
- **Hadoop**: Uses shell scripts for workflow starter check and datetime extraction
- **Databricks**: Uses Databricks notebooks for all operations
- **Hadoop**: Email notifications for success
- **Databricks**: No email notifications, uses Cosmos DB for logging and notifications
- **Hadoop**: Uses HDFS for data storage
- **Databricks**: Uses ADLS (Azure Data Lake Storage) for data storage
- **Hadoop**: Uses MapR filesystem (maprfs://)
- **Databricks**: Uses Azure storage (abfss://)
- **Hadoop**: Workflow starter check with lock file mechanism
- **Databricks**: No workflow starter check, uses Cosmos DB notifications
- **Hadoop**: Separate sub-workflow for each table ingestion
- **Databricks**: Single notebook handles all tables with internal parallelization
- **Hadoop**: Includes Spark JDBC sub-workflow for specific tables (hospitalpurge and others)
- **Databricks**: All tables handled uniformly in single extract notebook
