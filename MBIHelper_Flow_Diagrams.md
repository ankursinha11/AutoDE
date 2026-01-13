# MBI Helper Flow Diagrams

## Hadoop Workflow 1: escan_coverage_helper : > build repo

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
│ Check        │    │ End         │
│ Notification │    └─────────────┘
│ (Shell -     │
│  medicarecover│
│  agediscovery│
│  to          │
│  policyhelpers)│
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
│ Log Workflow Start          │
│ (Spark - MapR DB)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Populate Repo Staging       │
│ (Spark - build helper repo)│
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check Staging               │
│ (Shell script)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Publish?          │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    │
┌─────────────┐          │
│ Publish      │          │
│ Helper Repo  │          │
│ (Shell -     │          │
│  merge staging│          │
│  to publish) │          │
└──────┬──────┘          │
       │                 │
       └──────────┬───────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Create Notification         │
│ (Shell - create notification│
│  file for policyhelpers)   │
└──────────┬──────────────────┘
           │
           ├──────────────────────────────────────────────────────┐
           │                                                      │
           ▼                                                      ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Log Workflow     │  │ Remove Message   │  │ Email Notify     │
│ Finish           │  │ Done             │  │ Success          │
│ (Spark - MapR DB)│  │ (Shell)          │  │                  │
└──────┬───────────┘  └──────┬───────────┘  └──────┬───────────┘
       │                     │                     │
       └──────────┬──────────┴─────────────────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Error Check                 │
└───┬────────────────────┬────┘
    │ No Error            │ Error
    ▼                     ▼
┌─────────────┐    ┌──────────────────┐
│ End          │    │ Fork: Log Fail, │
└─────────────┘    │ Email Fail      │
                   │ (Parallel)      │
                   └──────┬──────────┘
                          │
                          ▼
                   ┌─────────────┐
                   │ End         │
                   └─────────────┘
```

## Hadoop Workflow 2: escan_coverage_helpers : medicare_helper_selectaccts

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
│ Check        │    │ End         │
│ Notification │    │             │
│ (Shell -     │    └─────────────┘
│  policyhelpers│
│  to          │
│  trackpolicyhelpers)│
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
│ Log Workflow Start          │
│ (Spark - MapR DB)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Populate Track Policy        │
│ Helpers                      │
│ (Spark - populate tracking   │
│  data)                       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Select Helper Accounts       │
│ (Spark - select accounts     │
│  for MBI generation)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Sqoop Out                   │
│ (Export to SQL Server       │
│  staging table)             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Notification         │
│ (Shell - create notification│
│  file for trackpolicyhelpers)│
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Log Workflow     │  │ Email Notify     │
│ Finish           │  │ Success          │
│ (Spark - MapR DB)│  │                  │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Error Check                 │
└───┬────────────────────┬────┘
    │ No Error           │ Error
    ▼                    ▼
┌─────────────┐    ┌──────────────────┐
│ End          │    │ Fork: Log Fail,  │
└─────────────┘    │ Email Fail      │
                   │ (Parallel)      │
                   └──────┬──────────┘
                          │
                          ▼
                   ┌─────────────┐
                   │ End         │
                   └─────────────┘
```

## Databricks Pipeline: pl_mbihelper

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Get Breadcrumb   │  │                  │
│ (Query Cosmos DB │  │                  │
│  for notification│  │                  │
│  type:           │  │                  │
│  data_ingestion_ │  │                  │
│  patientaccts5)  │  │                  │
└──────┬───────────┘  └──────────────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set Breadcrumb   │  │ Set Pipeline     │
│ Variable         │  │ Return Value     │
│                  │  │                  │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       │                     │
       ├─────────────────────┤
       │                     │
       ▼                     ▼
┌──────────────────┐  ┌──────────────────┐
│ Log Workflow     │  │ Update          │
│ Start            │  │ Notification     │
│ (Cosmos DB)      │  │ In Progress     │
│                  │  │ (Cosmos DB)      │
└──────┬───────────┘  └──────┬──────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Populate Repo                │
│ (Build helper repo from      │
│  data ingestion and GMRN     │
│  data)                       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Coverage Helper              │
│ Publish Data                 │
│ (Publish helper repo to     │
│  served location)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Populate Tracker            │
│ (Populate tracking data     │
│  for policy helpers)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Select Accounts             │
│ (Select accounts for MBI     │
│  generation)                 │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Sqoop Notebook              │
│ (Export to SQL Server       │
│  PatientAcctXLead table)   │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ DB Sharding      │  │ Shards Move      │
│ (Inactive)       │  │ (Inactive)       │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Delete Trigger File         │
│ (Delete ADLS trigger file)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Notification          │
│ Completed                    │
│ (Update Cosmos DB runstatus │
│  to processed)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Key Process Steps

### Hadoop Workflow 1: escan_coverage_helper : > build repo

1. **Check Previous Workflow Status** - Spark job to check MapR DB for failed workflows
2. **Restart Previous Failed Workflow** - Shell script to restart failed workflows
3. **Decision: Restart Successful?** - If restart successful, continue; else end
4. **Check Notification** - Shell script to check for notification file from medicarecoveragediscovery to policyhelpers
5. **Decision: Notification Exists?** - If notification exists, continue; else end
6. **Get DateTime** - Shell script to extract breadcrumb date from notification
7. **Log Workflow Start** - Spark job to log RUNNING status to MapR DB
8. **Populate Repo Staging** - Spark job (populaterepo.py) to build helper repo from data ingestion data
9. **Check Staging** - Shell script to check if staging data exists
10. **Decision: Publish?** - If publish=true, publish; else skip to notification
11. **Publish Helper Repo** - Shell script to merge staging data to publish location
12. **Create Notification** - Shell script to create notification file for policyhelpers
13. **Fork: Log Finish, Remove Message Done, Email Notify** - Parallel execution of logging, cleanup, and email
14. **Error Check** - Check for errors in parallel actions
15. **End or Email Fail** - End workflow or send failure email

### Hadoop Workflow 2: escan_coverage_helpers : medicare_helper_selectaccts

1. **Check Previous Workflow Status** - Spark job to check MapR DB for failed workflows
2. **Restart Previous Failed Workflow** - Shell script to restart failed workflows
3. **Decision: Restart Successful?** - If restart successful, continue; else end
4. **Check Notification** - Shell script to check for notification file from policyhelpers to trackpolicyhelpers
5. **Decision: Notification Exists?** - If notification exists, continue; else end
6. **Get DateTime** - Shell script to extract breadcrumb date from notification
7. **Log Workflow Start** - Spark job to log RUNNING status to MapR DB
8. **Populate Track Policy Helpers** - Spark job (populatetracker.py) to populate tracking data for policy helpers
9. **Select Helper Accounts** - Spark job (selectaccounts.py) to select accounts for MBI generation
10. **Sqoop Out** - Sqoop action to export selected accounts to SQL Server staging table (hdppacctstobegeneratedmcare)
11. **Create Notification** - Shell script to create notification file for trackpolicyhelpers
12. **Fork: Log Finish, Email Notify** - Parallel execution of logging and email
13. **Error Check** - Check for errors in parallel actions
14. **End or Email Fail** - End workflow or send failure email

### Databricks Pipeline: pl_mbihelper

1. **Get Breadcrumb** - Databricks notebook to query Cosmos DB for unprocessed notification with type data_ingestion_patientaccts5
2. **Set Breadcrumb Variable** - Set breadcrumb variable from get_breadcrumb output
3. **Set Pipeline Return Value** - Set pipeline return value with breadcrumb (parallel with Set Breadcrumb)
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360 (parallel with update notification)
5. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running" (parallel with log workflow start)
6. **Populate Repo** - Databricks notebook (populaterepo) to build helper repo from data ingestion and GMRN data
7. **Coverage Helper Publish Data** - Databricks notebook (coverage_helper_publish_data) to publish helper repo to served location
8. **Populate Tracker** - Databricks notebook (populatetracker) to populate tracking data for policy helpers
9. **Select Accounts** - Databricks notebook (selectaccounts) to select accounts for MBI generation
10. **Sqoop Notebook** - Databricks notebook (mbi_helper_sqoop_out) to export selected accounts to SQL Server PatientAcctXLead table
11. **DB Sharding** - Inactive Databricks notebook for database sharding
12. **Shards Move** - Inactive Databricks notebook for moving shards
13. **Delete Trigger File** - Delete ADLS trigger file that started this pipeline
14. **Update Notification Completed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Two separate workflows (build repo and select accounts)
- **Databricks**: Single pipeline combining both workflows
- **Hadoop Workflow 1**: Builds helper repo, checks staging, conditionally publishes, creates notification for policyhelpers
- **Hadoop Workflow 2**: Populates tracker, selects accounts, exports to SQL Server, creates notification for trackpolicyhelpers
- **Databricks**: Combines both workflows - builds repo, publishes, populates tracker, selects accounts, exports to SQL Server
- **Hadoop**: Uses shell scripts for file operations and notifications
- **Databricks**: Uses Databricks notebooks for all operations
- **Hadoop**: Uses Spark jobs for data processing
- **Databricks**: Uses Databricks notebooks for data processing
- **Hadoop**: Uses Sqoop action for SQL Server export
- **Databricks**: Uses Databricks notebook (mbi_helper_sqoop_out) for SQL Server export
- **Hadoop**: Uses MapR DB for logging and HDFS for notifications
- **Databricks**: Uses Cosmos DB for logging and notifications
- **Hadoop**: Email notifications for success and failure
- **Databricks**: No email notifications, uses Cosmos DB status updates
- **Hadoop**: Fork/join for parallel execution of logging and email
- **Databricks**: Parallel execution of logging and notification updates, then sequential processing
- **Hadoop**: Decision nodes for conditional execution
- **Databricks**: Sequential execution with conditional logic in notebooks
- **Hadoop**: Uses HDFS for data storage
- **Databricks**: Uses ADLS (Azure Data Lake Storage) for data storage
- **Databricks**: Includes inactive sharding steps (DB Sharding, Shards Move)
- **Databricks**: Deletes trigger file at end
- **Databricks**: Gets breadcrumb from Cosmos DB notification instead of shell script
