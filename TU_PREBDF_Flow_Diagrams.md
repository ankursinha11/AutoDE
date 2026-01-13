# TU-PREBDF Flow Diagrams

## Hadoop Workflow: TU-PREBDF

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Date                    │
│ (Shell script - extract     │
│  breadcrumb date)           │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ TU Download      │    │ Email Fail       │
│ (Shell script -  │    │ (Error path)     │
│  download TU     │    │                  │
│  data files)     │    └──────────────────┘
└──────┬───────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ TU Add BC        │    │ Email Fail       │
│ (Pig script -     │    │ (Error path)     │
│  add breadcrumb  │    │                  │
│  to data)        │    └──────────────────┘
└──────┬───────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ TU Publish       │    │ Email Fail       │
│ Demo Data        │    │ (Error path)     │
│ (Pig script -    │    │                  │
│  publish demo    │    └──────────────────┘
│  data)           │
└──────┬───────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ Email Success    │    │ Email Fail       │
│                  │    │ (Error path)     │
└──────┬───────────┘    └──────┬───────────┘
       │                      │
       └──────────┬───────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Databricks Pipeline: pl_tu_prebdf

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
│ Set BC Variable  │  │ Set Pipeline     │
│                  │  │ Return Value     │
│                  │  │                  │
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
│ Move TU Files               │
│ (Move TU data files from    │
│  source to destination      │
│  location)                  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Wait                        │
│ (Wait 1 second)             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Data With BC            │
│ (Get TU data with           │
│  breadcrumb, add BC         │
│  to data)                   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Publish Data                │
│ (Publish demo data to       │
│  served location)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Notification         │
│ Processed                   │
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

### Hadoop Workflow Steps:

1. **Get Date** - Shell script (get_date.sh) to extract breadcrumb date
2. **TU Download** - Shell script (get_tu.sh) to download TU data files from source location
3. **TU Add BC** - Pig script (bc.pig) to add breadcrumb to downloaded TU data
4. **TU Publish Demo Data** - Pig script (demoData_publish.pig) to publish demo data to served location
5. **Email Success** - Send success email to pdlhccdd@transunion.com
6. **Email Fail** - Send failure email with error details (error path from any step)

### Databricks Pipeline Steps:

1. **Get BC** - Databricks notebook (get_bc) to get breadcrumb date in YMDT-H format
2. **Set BC Variable** - Set breadcrumb variable from get_bc output
3. **Set Pipeline Return Value** - Set pipeline return value with breadcrumb (parallel with Set BC Variable)
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360 (parallel with update notification)
5. **Update Notification Running** - Update Cosmos DB runstatus table to "running" for tu_prebdf_scheduletrigger (parallel with log workflow start)
6. **Move TU Files** - Databricks notebook (move_tu_files) to move TU data files from read container to destination location
7. **Wait** - Wait activity (1 second) after move_tu_files completes
8. **Get Data With BC** - Databricks notebook (get_bc_data) to get TU data with breadcrumb, add BC to data
9. **Publish Data** - Databricks notebook (demo_data_publish) to publish demo data to served location
10. **Update Notification Processed** - Update Cosmos DB runstatus table to "processed" for tu_prebdf_scheduletrigger

## Key Differences

- **Hadoop**: Uses shell scripts for date extraction and file download
- **Databricks**: Uses Databricks notebook (get_bc) for breadcrumb extraction
- **Hadoop**: Uses shell script (get_tu.sh) for downloading TU files
- **Databricks**: Uses Databricks notebook (move_tu_files) for moving TU files from ADLS container
- **Hadoop**: Uses Pig scripts (bc.pig, demoData_publish.pig) for data processing
- **Databricks**: Uses Databricks notebooks (get_bc_data, demo_data_publish) for data processing
- **Hadoop**: Sequential execution of all steps
- **Databricks**: Parallel execution of logging and notification updates, then sequential processing
- **Hadoop**: Email notifications for success and failure
- **Databricks**: No email notifications, uses Cosmos DB for logging and notifications
- **Hadoop**: Error handling via email on failure
- **Databricks**: Error handling via Cosmos DB status updates
- **Hadoop**: Uses HDFS for data storage
- **Databricks**: Uses ADLS (Azure Data Lake Storage) for data storage
- **Hadoop**: No wait activity between steps
- **Databricks**: Includes Wait activity (1 second) after move_tu_files
- **Hadoop**: Direct file download from source
- **Databricks**: File movement from read container (cda-data) to destination location
- **Hadoop**: Uses MapR filesystem (maprfs://)
- **Databricks**: Uses Azure storage (abfss://)
