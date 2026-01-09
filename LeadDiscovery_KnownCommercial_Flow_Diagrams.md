# Lead Discovery Known Commercial Flow Diagrams

## Hadoop Workflow: leaddiscovery:known_commercial

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
│ Known Commercial Daily       │
│ Runcheck                     │
│ (Check if already run today)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Log Workflow Start           │
│ (MapR DB oozie_360)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Fetch Hospital Configuration │
│ (Sqoop import - hospitals   │
│  with KC enabled)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ HDFS Dir Check               │
│ Sqoop KC Enabled Config       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Config Data        │
│ Exists?                      │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Known        │    │ Log        │
│ Commercial   │    │ Workflow    │
│ Extract Data │    │ Finish      │
│ (Shell)      │    └──────┬──────┘
└──────┬──────┘           │
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ Known Commercial Process    │
│ Data                         │
│ (Process patient accounts    │
│  codes, boundary conditions)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Known Commercial Filter      │
│ Data                         │
│ (Filter based on config      │
│  values)                      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Known Commercial Publish    │
│ Data                         │
│ (Shell - merge with delta    │
│  table, prepare sqoop data)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Log Notification             │
│ Known Commercial             │
│ (MapR DB runstatus table)    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ HDFS Dir Check Leads         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Leads Data         │
│ Exists?                      │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Out   │    │ Log        │
│ to          │    │ Workflow    │
│ hdppatient  │    │ Finish      │
│ acctxops    │    └──────┬──────┘
└──────┬──────┘           │
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ Sqoop Out to                 │
│ hdppatientacctxopsbatch       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Log Workflow Finish          │
│ (MapR DB oozie_360)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Email Notify                 │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Databricks Pipeline: pl_leaddiscovery_known_commercial

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Breadcrumb               │
│ (Query Cosmos DB for         │
│  unprocessed notifications)  │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set Breadcrumb   │  │ Breadcrumb       │
│ Variable         │  │ Pipeline Return │
│                  │  │ Value            │
└──────┬───────────┘  └──────────────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Log Workflow     │  │ Update          │
│ Start            │  │ Notification    │
│ (Cosmos DB)      │  │ In Progress     │
└──────┬───────────┘  └──────────────────┘
       │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Sqoop Input                  │
│ (Notebook - import hospital  │
│  config with KC enabled      │
│  from SQL Server)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Known Commercial Extract     │
│ Data                         │
│ (Extract data from           │
│  cloudmigration tables)      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Known Commercial Process     │
│ Data                         │
│ (Process patient accounts    │
│  codes, boundary conditions)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Known Commercial Filter      │
│ Data                         │
│ (Filter based on config      │
│  values)                      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Known Commercial Publish    │
│ Data                         │
│ (Merge with delta table,     │
│  prepare sqoop data,         │
│  return count)               │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set check_leads   │  │ Log Notification │
│ ret_val           │  │ for Leadlookup   │
│ (From publish     │  │ KC               │
│  output)          │  │                  │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Check Sqoop Output Exists    │
│ (IfCondition: check_leads   │
│  ret_val == 1?)             │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    │
┌─────────────┐          │
│ Sqoop Out   │          │
│ (Notebook - │          │
│  export to   │          │
│  SQL Server)│          │
└──────┬──────┘          │
       │                 │
       └──────────┬──────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Create Trigger for           │
│ Leadlookup KC                │
│ (Create ADLS trigger file)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Notification           │
│ Completed                     │
│ (Update Cosmos DB runstatus) │
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
4. **Known Commercial Daily Runcheck** - Check if workflow already ran today
5. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
6. **Fetch Hospital Configuration** - Sqoop import hospitals with KC enabled flag from SQL Server
7. **HDFS Dir Check Sqoop KC Enabled Config** - Check if hospital config data exists
8. **Known Commercial Extract Data** - Shell script to extract data from various tables (patient accounts, codes, hospitals, etc.)
9. **Known Commercial Process Data** - Process patient account codes, create boundary conditions, cross-reference data
10. **Known Commercial Filter Data** - Filter data based on config values and boundary conditions
11. **Known Commercial Publish Data** - Shell script to merge with delta table, prepare final output and sqoop data
12. **Log Notification Known Commercial** - Log notification to MapR DB runstatus table and HDFS notification directory
13. **HDFS Dir Check Leads** - Check if leads data exists for sqoop export
14. **Sqoop Out to hdppatientacctxops** - Export patient accounts to SQL Server table
15. **Sqoop Out to hdppatientacctxopsbatch** - Export batch breadcrumb to SQL Server table
16. **Log Workflow Finish** - Log FINISHED status to MapR DB
17. **Email Notify** - Send success email

### Databricks Pipeline Steps:

1. **Get Breadcrumb** - Query Cosmos DB for unprocessed notifications (using get_bc notebook)
2. **Set Breadcrumb Variable** - Extract and set breadcrumb from notification
3. **Breadcrumb Pipeline Return Value** - Set pipeline return value with breadcrumb
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360
5. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running" (using log_notification notebook)
6. **Sqoop Input** - Import hospital configuration with KC enabled from SQL Server using notebook (knowncommercial_import)
7. **Known Commercial Extract Data** - Extract data from cloudmigration tables (patient accounts, codes, hospitals, etc.)
8. **Known Commercial Process Data** - Process patient account codes, create boundary conditions, cross-reference data
9. **Known Commercial Filter Data** - Filter data based on config values and boundary conditions
10. **Known Commercial Publish Data** - Merge with delta table, prepare final output and sqoop data, return count
11. **Set check_leads_ret_val** - Set variable from publish_data output (count of records)
12. **Check Sqoop Output Exists** - IfCondition activity checking if check_leads_ret_val == 1
13. **Sqoop Out** - If condition true, export to SQL Server using notebook (knowncommercial_export)
14. **Log Notification for Leadlookup KC** - Log notification to Cosmos DB runstatus table with notification type "knowncommercial"
15. **Create Trigger for Leadlookup KC** - Create ADLS trigger file for downstream leadlookup pipeline
16. **Update Notification Completed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Uses Sqoop action to fetch hospital configuration
- **Databricks**: Uses Databricks notebook (knowncommercial_import) to fetch hospital configuration
- **Hadoop**: Uses shell scripts for extract_data and publish_data
- **Databricks**: Uses Databricks notebooks for extract_data and publish_data
- **Hadoop**: Conditional sqoop export based on HDFS directory check
- **Databricks**: Conditional sqoop export based on publish_data return value (check_leads_ret_val)
- **Hadoop**: Logs notification after publish_data, before sqoop export
- **Databricks**: Logs notification after conditional sqoop check, before creating trigger
- **Hadoop**: Creates notification file in HDFS notification directory
- **Databricks**: Creates trigger file in ADLS triggers directory
- **Hadoop**: Uses MapR DB for logging and notifications
- **Databricks**: Uses Cosmos DB for logging and notifications
- **Hadoop**: Includes daily runcheck to prevent duplicate runs
- **Databricks**: No explicit daily runcheck (handled by notification system)
- **Hadoop**: Separate sqoop actions for main table and batch table
- **Databricks**: Single sqoop notebook handles both exports
- **Hadoop**: Email notification at end
- **Databricks**: No email notification
- **Hadoop**: Publish data uses shell script with delta table merge
- **Databricks**: Publish data uses notebook with delta table merge and returns count
