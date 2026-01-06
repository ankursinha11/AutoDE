# Family Clustering Flow Diagrams

## Hadoop Workflow: cdd: family_clustering

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
│ Decision: Restart           │
│ Successful?                 │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get         │    │ End         │
│ Notification│    └─────────────┘
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Check Notification          │
│ (Read HDFS notification)    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Notification      │
│ Available?                  │
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
│ Log Workflow Start          │
│ (MapR DB oozie_360)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Source Type       │
│ es_xref_famc?               │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    │
┌────────────────────┐   │
│ Get Unique PermID  │   │
│ Relations          │   │
│ (From escan data)  │   │
└──────┬─────────────┘   │
       │                 │
       └────────┬────────┘
                │
                ▼
┌─────────────────────────────┐
│ Propagate Policy Info       │
│ to PermID Relations         │
│ (Join relations with        │
│  lr_xref_permid_transaction│
│  and create candidates)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Fork: Parallel Execution    │
└───┬───┬───┬───┬─────────────┘
    │   │   │   │
    ▼   ▼   ▼   ▼
┌───┐ ┌───┐ ┌───┐ ┌──────────┐
│Log│ │Log│ │Not│ │Update    │
│Fin│ │Not│ │ify│ │Notif     │
│ish│ │if │ │LR │ │          │
└───┘ └───┘ └───┘ └──────────┘
    │   │   │   │
    └───┴───┴───┴───┐
                    │
                    ▼
            ┌───────────────┐
            │ Join: All     │
            │ Complete      │
            └───────┬───────┘
                    │
                    ▼
            ┌───────────────┐
            │ Decision:     │
            │ Errors?       │
            └───┬───────┬───┘
                │ No    │ Yes
                ▼       ▼
        ┌──────────┐ ┌──────────┐
        │ End      │ │ Log Fail │
        └──────────┘ └────┬─────┘
                          │
                          ▼
                  ┌──────────────┐
                  │ Email Failure│
                  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ End (Fail)   │
                  └──────────────┘
```

## Databricks Pipeline: pl_leadrepository_xref_famc

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
│ Set Breadcrumb   │  │ Set Notification │
│ Variable         │  │ Type Variable    │
└──────┬───────────┘  └────────┬─────────┘
       │                      │
       └──────────┬───────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Log Workflow Start           │
│ (Cosmos DB operations_log)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Notification          │
│ Status: In Progress          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Notification Type │
│ es_xref_famc?                │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    │
┌────────────────────┐   │
│ Get Unique PermID  │   │
│ Relations          │   │
│ (From escan data)  │   │
└──────┬─────────────┘   │
       │                 │
       └────────┬────────┘
                │
                ▼
┌─────────────────────────────┐
│ Propagate Policy Info        │
│ to PermID Relations          │
│ (Join relations with         │
│  lr_xref_permid_transaction │
│  includes hospitalfk,        │
│  creates candidates)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create LR XRef PermID        │
│ (Transform candidates,       │
│  add metadata, write to     │
│  famc_lr_xref_permid_        │
│  transaction table)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Notification Type  │
│ es_xref_famc?                │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    │
┌────────────────────┐   │
│ Log Notification   │   │
│ (TU Source Family  │   │
│  Member Link)      │   │
└──────┬─────────────┘   │
       │                 │
       └────────┬────────┘
                │
                ▼
┌─────────────────────────────┐
│ Log Notification             │
│ (LSB FamC)                   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Trigger File           │
│ (LSB FamC)                   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Delete Trigger File          │
│ (Original trigger)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Notification          │
│ Status: Processed             │
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
3. **Get Notification** - Query MapR DB runstatus table for notifications
4. **Check Notification** - Read HDFS notification file to determine if processing should start
5. **Get Date** - Extract breadcrumb (date) from notification
6. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
7. **Get Unique PermID Relations** (Conditional) - Extract unique PermID relations from escan data (only for es_xref_famc)
8. **Propagate Policy Info to PermID Relations** - Join PermID relations with lr_xref_permid_transaction to create candidate transactions
9. **Log Workflow Finish** - Log FINISHED status to MapR DB
10. **Log Notification** - Update HDFS notification file
11. **Notify Lead Repository** - Create notification in MapR DB for lead repository
12. **Update Notification** - Update MapR DB runstatus table
13. **Email Notify** - Send success email

### Databricks Pipeline Steps:

1. **Get Breadcrumb** - Query Cosmos DB runstatus table for unprocessed notifications (fc_xref_famc, ie_xref_famc, es_xref_famc)
2. **Set Breadcrumb Variable** - Extract and set breadcrumb from notification
3. **Set Notification Type Variable** - Extract and set notification type
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360
5. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running"
6. **Get Unique PermID Relations** (Conditional) - Extract unique PermID relations from escan data (only for es_xref_famc)
7. **Propagate Policy Info to PermID Relations** - Join PermID relations with lr_xref_permid_transaction (includes hospitalfk), creates candidate transactions
8. **Create LR XRef PermID** - Transform candidates, add metadata (_id, xrefsource, audittrail), write to famc_lr_xref_permid_transaction Delta table
9. **Log Notification (TU)** (Conditional) - Log notification for TU Source Family Member Link (only for es_xref_famc)
10. **Log Notification (LSB)** - Log notification for LSB FamC
11. **Create Trigger File** - Create trigger file for downstream processing
12. **Delete Trigger File** - Delete original trigger file
13. **Update Notification Processed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Does not include the "Create LR XRef PermID" step (this is in a separate workflow)
- **Databricks**: Includes "Create LR XRef PermID" step within the same pipeline
- **Hadoop**: Uses MapR DB and HDFS for notifications
- **Databricks**: Uses Cosmos DB and ADLS for notifications
- **Databricks**: Includes hospitalfk in candidate transactions filtering
- **Hadoop**: Does not include hospitalfk in candidate transactions filtering

