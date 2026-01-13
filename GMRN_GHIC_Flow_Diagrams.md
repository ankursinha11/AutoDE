# GMRN GHIC Flow Diagrams

## Hadoop Workflow: escan_globalmrn : ghic

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Oozie WF Starter Check      │
│ (Shell script - check if    │
│  workflow can start)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Can Start?        │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Check       │    │ End         │
│ Previous WF │    └─────────────┘
│ Status      │
│ (Spark)     │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Restart     │    │ Email Fail  │
│ Previous    │    │             │
│ Failed WF   │    └─────────────┘
│ (Shell)     │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Decision: Restart            │
│ Successful?                 │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Check       │    │ End         │
│ Notification│    └─────────────┘
│ (Shell)     │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Decision: Notification      │
│ Exists?                     │
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
│ Populate GMRN GHIC Insert   │
│ (Spark - create insert      │
│  records)                   │
└──────┬──────────────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Populate   │    │ Email Fail  │
│ GMRN GHIC  │    │             │
│ Update     │    └─────────────┘
│ (Spark -   │
│  create    │
│  update,   │
│  delete,   │
│  history)  │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Populate   │    │ Email Fail  │
│ GMRN GHIC  │    │             │
│ ID         │    └─────────────┘
│ (Spark -   │
│  assign    │
│  PKs)      │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Data │    │ Email Fail  │
│ (Spark -   │    │             │
│  format    │    └─────────────┘
│  for sqoop)│
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Out   │    │ Email Fail  │
│ (Sqoop -    │    │             │
│  export to  │    └─────────────┘
│  SQL Server)│
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Intermediate│    │ Email Fail  │
│ GMRN HIC    │    │             │
│ Data        │    └─────────────┘
│ (Shell -    │
│  merge data)│
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Create      │    │ Email Fail  │
│ Prepublish  │    │             │
│ File GHIC   │    └─────────────┘
│ (Spark)     │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Publish     │    │ Email Fail  │
│ Data GHIC   │    │             │
│ (Shell)     │    └─────────────┘
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Log Notification            │
│ (Shell)                     │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Decision: Job Done?         │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Fork:      │    │ Email Fail  │
│ Success    │    │             │
│ (Parallel) │    └─────────────┘
└───┬────┬───┘
    │    │
    ▼    ▼
┌─────────────┐    ┌─────────────┐
│ Log to     │    │ Email       │
│ MapR DB    │    │ Success Sent│
│ Success    │    │             │
│ (Spark)    │    │             │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
       ┌─────────────────────────────┐
       │ End                         │
       └─────────────────────────────┘
```

## Databricks Pipeline: pl_gmrn_ghic

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Breadcrumb              │
│ (Databricks notebook -      │
│  query Cosmos DB for        │
│  notification)              │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ Set Breadcrumb   │    │ Breadcrumb      │
│ (SetVariable)    │    │ Pipeline Return │
│                  │    │ Value           │
│                  │    │ (SetVariable)   │
└──────┬───────────┘    └──────┬───────────┘
       │                       │
       ├───────────────────────┼──────────┐
       │                       │          │
       ▼                       ▼          ▼
┌──────────────────┐    ┌──────────────────┐
│ 360 Logger       │    │ Update          │
│ Running          │    │ Notification    │
│ (Databricks)     │    │ Inprogress      │
│                  │    │ (Databricks)    │
└──────┬───────────┘    └──────┬───────────┘
       │                       │
       └───────────┬───────────┘
                   │
                   ▼
          ┌─────────────────────────────┐
          │ GMRN GHIC                   │
          │ (Databricks notebook -      │
          │  create insert records)     │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Create Delta Update         │
          │ File HIC                    │
          │ (Databricks notebook -     │
          │  create update, delete,    │
          │  history records)           │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ GMRN XHIC Identity          │
          │ (Databricks notebook -     │
          │  assign primary keys)       │
          └──────┬──────────────────┘
                 │
                 ├──────────────────┐
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Create     │    │ (Inactive)   │
          │ Sqoop      │    │ Sqoop       │
          │ (Databricks│    │ Notebook    │
          │  - format  │    │             │
          │  for sqoop)│    │             │
          │ (Inactive) │    │             │
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌─────────────────────────────┐
                 │ Merge Data                  │
                 │ (Databricks notebook -      │
                 │  merge inserts and deletes) │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ Ingest Table                │
                 │ (Databricks notebook -     │
                 │  create prepublish files)   │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ Publish Data               │
                 │ (Databricks notebook -     │
                 │  publish to served location)│
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ Delete Trigger File        │
                 │ (Databricks notebook)       │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ Update Notification         │
                 │ Completed                   │
                 │ (Databricks notebook)      │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ End                         │
                 └─────────────────────────────┘
```

## Key Process Steps

### Hadoop Workflow: escan_globalmrn : ghic

1. **Oozie WF Starter Check** - Shell script to check if workflow can start (checks lock file)
2. **Decision: Can Start?** - If check returns true, continue; else end
3. **Check Previous WF Status** - Spark job to check for previous failed workflow runs
4. **Restart Previous Failed WF** - Shell script to restart failed workflow if found
5. **Decision: Restart Successful?** - If restart successful, continue; else end
6. **Check Notification** - Shell script to check for notification from upstream workflow (ghic)
7. **Decision: Notification Exists?** - If notification exists, continue; else end
8. **Get DateTime** - Shell script to extract breadcrumb date from notification
9. **Populate GMRN GHIC Insert** - Spark job to create insert records for GlobalMRNxHospInsuranceCodes
10. **Populate GMRN GHIC Update** - Spark job to create update, delete, and history records
11. **Populate GMRN GHIC ID** - Spark job to assign primary keys to records
12. **Sqoop Data** - Spark job to format data for Sqoop export
13. **Sqoop Out** - Sqoop action to export data to SQL Server staging table
14. **Intermediate GMRN HIC Data** - Shell script to merge insert and delete data
15. **Create Prepublish File GHIC** - Spark job to create prepublish files
16. **Publish Data GHIC** - Shell script to publish data to served location
17. **Log Notification** - Shell script to log notification for downstream workflows
18. **Decision: Job Done?** - Check if notification was logged successfully
19. **Fork: Email Success** - Parallel execution of logging and email notification
20. **Log to MapR DB Success** - Spark job to log success to MapR DB
21. **Email Success Sent** - Send success email notification
22. **Join and End** - Wait for both parallel paths and end workflow

### Databricks Pipeline: pl_gmrn_ghic

1. **Get Breadcrumb** - Databricks notebook to query Cosmos DB for notification type "data_ingestion_patientaccts4" and retrieve breadcrumb date
2. **Set Breadcrumb** - Set pipeline variable 'dt' with breadcrumb value
3. **Breadcrumb Pipeline Return Value** - Set pipeline return value with breadcrumb (parallel with Set Breadcrumb)
4. **360 Logger Running** - Log RUNNING status to Cosmos DB operations_log_360 (parallel with update notification)
5. **Update Notification Inprogress** - Update Cosmos DB runstatus table to "running" (parallel with log workflow start)
6. **GMRN GHIC** - Databricks notebook to create insert records for GlobalMRNxHospInsuranceCodes
7. **Create Delta Update File HIC** - Databricks notebook to create update, delete, and history records
8. **GMRN XHIC Identity** - Databricks notebook to assign primary keys to records
9. **Create Sqoop** - Databricks notebook to format data for Sqoop export (Inactive)
10. **Sqoop Notebook** - Databricks notebook to export data to SQL Server using JDBC push (Inactive)
11. **Merge Data** - Databricks notebook to merge insert and delete data
12. **Ingest Table** - Databricks notebook to create prepublish files
13. **Publish Data** - Databricks notebook to publish data to served location
14. **Delete Trigger File** - Databricks notebook to delete trigger file
15. **Update Notification Completed** - Update Cosmos DB runstatus table to "processed"
16. **End** - Pipeline completion

## Key Differences

1. **Notification System**: Hadoop uses shell scripts with HDFS-based notifications; Databricks uses Databricks notebooks with Cosmos DB
2. **Breadcrumb Retrieval**: Hadoop gets breadcrumb from notification file via shell script; Databricks queries Cosmos DB via Databricks notebook
3. **Workflow Starter Check**: Hadoop has explicit starter check; Databricks relies on ADF scheduling
4. **Previous Workflow Check**: Hadoop checks and restarts previous failed workflows; Databricks relies on ADF error handling
5. **Data Processing**: Both use Spark/Databricks for main processing, but Hadoop uses separate Spark jobs while Databricks uses notebooks
6. **Sqoop Export**: Hadoop uses Sqoop action; Databricks has inactive Sqoop notebook (JDBC push alternative)
7. **Data Publishing**: Hadoop uses shell script; Databricks uses Databricks notebook
8. **Logging**: Hadoop uses Spark jobs to log to MapR DB; Databricks uses Databricks notebooks to log to Cosmos DB
9. **Error Handling**: Hadoop has email notifications on failure; Databricks relies on ADF error handling and Cosmos DB logging
10. **Parallel Execution**: Hadoop uses fork/join for parallel logging and email; Databricks uses parallel dependencies for logging and notification updates
11. **Notification Logging**: Hadoop uses shell script; Databricks updates Cosmos DB directly
12. **Trigger File Management**: Hadoop doesn't have trigger files; Databricks deletes trigger file after processing
