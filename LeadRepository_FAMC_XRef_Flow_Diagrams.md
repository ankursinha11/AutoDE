# Lead Repository Family Clustering XRef Flow Diagrams

## Hadoop Workflow: leadrepository : famc_xref_table_update

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
│ Get         │    │ End         │
│ Notification│    └─────────────┘
│ (MapR DB)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Check Notification         │
│ (Shell script)             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Notification     │
│ Exists?                     │
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
│ Log Workflow Start           │
│ (MapR DB oozie_360)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ LR XRef PermID Insert        │
│ (Transform candidate PermID  │
│  transactions to LR xref     │
│  format)                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Publish FAMC PermID          │
│ (Shell - merge with delta    │
│  table, publish to served)   │
└──────────┬──────────────────┘
           │
           ├──────────────────────────────────────────────────────┐
           │                                                      │
           ▼                                                      ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Log Workflow     │  │ Log Notification │  │ LSB Notification │  │ Update           │  │ Email Notify     │
│ Finish           │  │                  │  │                  │  │ Notification     │  │                  │
│ (MapR DB)        │  │ (HDFS)           │  │ (MapR DB)         │  │ (MapR DB)        │  │                  │
└──────┬───────────┘  └──────┬───────────┘  └──────┬───────────┘  └──────┬───────────┘  └──────┬───────────┘
       │                     │                     │                     │                     │
       └──────────┬──────────┴─────────────────────┴─────────────────────┴─────────────────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Error Check                 │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Databricks Pipeline: pl_leadrepository_xref_famc

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Breadcrumb               │
│ (Query Cosmos DB for         │
│  unprocessed notifications   │
│  with types:                 │
│  fc_xref_famc,               │
│  ie_xref_famc, es_xref_famc) │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set Breadcrumb   │  │ Pipeline Return  │
│ Variable         │  │ Values           │
│                  │  │                  │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       │                     │
       ▼                     ▼
┌──────────────────┐  ┌──────────────────┐
│ Set              │  │                  │
│ Notification Type│  │                  │
│ Variable         │  │                  │
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
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ If Condition for            │
│ es_xref_famc                │
│ (If notification_type ==    │
│  es_xref_famc)              │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    │
┌─────────────┐          │
│ Famc Unique │          │
│ PermID      │          │
│ Relations   │          │
│ (Get unique │          │
│  PermID      │          │
│  relations)  │          │
└──────┬──────┘          │
       │                 │
       └──────────┬───────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Famc Propagate Policyinfo   │
│ to PermID Relations         │
│ (Propagate policy info      │
│  to PermID relations,       │
│  handle delta runs)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Famc LR Create XRef Perm    │
│ (Transform candidate PermID  │
│  transactions to LR xref     │
│  format, include hospitalfk │
│  in _id generation)          │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ If Condition for │  │ Log Notification │
│ Logging TU       │  │ LSB FAMC         │
│ Sourced Family   │  │                  │
│ Member Link      │  │                  │
│ (If es_xref_famc)│  │                  │
└───┬──────────────┘  └──────┬───────────┘
    │ Yes                     │
    ▼                         │
┌─────────────┐               │
│ Log         │               │
│ Notification│               │
│ TU Sourced  │               │
│ Family      │               │
│ Member Link │               │
└──────┬──────┘               │
       │                      │
       ▼                      │
┌─────────────────────────────┐
│ Create Trigger File          │
│ TU Sourced Family            │
│ Member Link                  │
└──────┬──────────────────┘
       │
       └──────────┬───────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Create Trigger File          │
│ LSB FAMC                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Delete Trigger File          │
│ (Delete ADLS trigger file)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Notification          │
│ Processed                    │
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
3. **Get Notification** - Get notification from MapR DB runstatus table for famc_xref
4. **Check Notification** - Shell script to check if notification file exists in HDFS
5. **Get Date** - Extract breadcrumb (date) from notification
6. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
7. **LR XRef PermID Insert** - Transform candidate PermID transactions into LR xref format (famc_lr_create_xref_perm.py)
8. **Publish FAMC PermID** - Shell script to merge with delta table and publish to served location
9. **Log Workflow Finish** - Log FINISHED status to MapR DB (parallel with other actions)
10. **Log Notification** - Log notification to HDFS notification directory (parallel)
11. **LSB Notification** - Log notification to MapR DB for Lead Service Base (parallel)
12. **Update Notification** - Update MapR DB runstatus table (parallel)
13. **Email Notify** - Send success email (parallel)
14. **Error Check** - Check for errors in parallel actions

### Databricks Pipeline Steps:

1. **Get Breadcrumb** - Query Cosmos DB for unprocessed notifications with notification types: fc_xref_famc, ie_xref_famc, es_xref_famc (using get_breadcrumb_multiple_notifications_notificationtype notebook)
2. **Set Breadcrumb Variable** - Extract and set breadcrumb from notification
3. **Pipeline Return Values** - Set pipeline return values with breadcrumb and notification_type
4. **Set Notification Type Variable** - Extract and set notification_type from notification
5. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360 (parallel with update notification)
6. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running" (parallel with log workflow start)
7. **If Condition for es_xref_famc** - IfCondition checking if notification_type == "es_xref_famc"
8. **Famc Unique PermID Relations** - If condition true, get unique PermID relations from PermID relations data
9. **Famc Propagate Policyinfo to PermID Relations** - Propagate policy information to PermID relations, handle delta runs, include hospitalfk
10. **Famc LR Create XRef Perm** - Transform candidate PermID transactions to LR xref format, include hospitalfk in _id generation
11. **If Condition for Logging TU Sourced Family Member Link** - IfCondition checking if notification_type == "es_xref_famc"
12. **Log Notification TU Sourced Family Member Link** - If condition true, log notification for TU sourced family member link
13. **Create Trigger File TU Sourced Family Member Link** - If condition true, create ADLS trigger file for downstream pipeline
14. **Log Notification LSB FAMC** - Log notification to Cosmos DB runstatus table for Lead Service Base
15. **Create Trigger File LSB FAMC** - Create ADLS trigger file for downstream LSB pipeline
16. **Delete Trigger File** - Delete ADLS trigger file that started this pipeline
17. **Update Notification Processed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Gets notification from MapR DB for single notification type
- **Databricks**: Gets breadcrumb from Cosmos DB for multiple notification types (fc_xref_famc, ie_xref_famc, es_xref_famc)
- **Hadoop**: Uses shell script (publish_xref.sh) for publishing data
- **Databricks**: Uses Databricks notebooks for all operations
- **Hadoop**: Single workflow handles one notification type at a time
- **Databricks**: Single pipeline handles multiple notification types with conditional logic
- **Hadoop**: Includes famc_lr_create_xref_perm step only
- **Databricks**: Includes conditional step for es_xref_famc (famc_unique_permid_relations) and additional conditional logic for TU sourced family member link
- **Hadoop**: Uses fork/join for parallel execution of notifications and logging
- **Databricks**: Uses sequential execution with conditional branches
- **Hadoop**: Logs notification to HDFS notification directory
- **Databricks**: Logs notification to Cosmos DB and creates ADLS trigger files
- **Hadoop**: Includes LSB notification step
- **Databricks**: Includes LSB notification step plus conditional TU sourced family member link notification
- **Hadoop**: Uses MapR DB for logging and notifications
- **Databricks**: Uses Cosmos DB for logging and notifications
- **Hadoop**: Email notification at end
- **Databricks**: No email notification
- **Hadoop**: Uses get_datetime.sh shell script for breadcrumb
- **Databricks**: Uses get_breadcrumb_multiple_notifications_notificationtype notebook for breadcrumb
- **Databricks**: Includes hospitalfk in _id generation in famc_lr_create_xref_perm
- **Databricks**: Handles delta runs in famc_propagate_policyinfo_to_permid_relations
