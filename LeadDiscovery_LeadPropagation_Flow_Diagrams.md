# Lead Discovery Lead Propagation Flow Diagrams

## Hadoop Workflow: leaddiscovery: lead propagation

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
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Check Notification           │
│ (Shell script check)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Notification      │
│ Found?                      │
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
│ Get Min/Max Dates            │
│ (Create lookup for admit     │
│  dates by hospital and       │
│  edipartnerfk)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get PA                       │
│ (Get patient accounts from   │
│  data ingestion publish)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Process Leads                │
│ (Process leads with lookup  │
│  data and lead status)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ HDFS Dir Check               │
│ (Check if leads output       │
│  directory exists)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Leads Found?      │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Check Leads │    │ Email       │
│             │    │ Notify      │
│             │    │ Success     │
└──────┬──────┘    └─────────────┘
       │
       ▼
┌─────────────────────────────┐
│ HDFS Dir Check Leads         │
│ (Check if sqoop output       │
│  directory exists)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Sqoop Output      │
│ Found?                      │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Out   │    │ Email       │
│ (Export to  │    │ Notify      │
│  SQL Server │    │ Success     │
│  hdppatient │    └─────────────┘
│  acctxlead) │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Sqoop Out HDPBatch           │
│ (Export to SQL Server        │
│  hdppatientacctxleadbatch)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Fork: Parallel Execution    │
└───┬────────────────────┬────┘
    │                    │
    ├────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Update      │  │ Log         │  │ Email        │
│ Notification│  │ Notification│  │ Notify       │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │               │                 │
       └───────────────┼─────────────────┘
                      │
                      ▼
              ┌───────────────┐
              │ Purge         │
              │ Intermediate  │
              │ Data          │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ Log Workflow  │
              │ Finish         │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ Join          │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ Decision:     │
              │ Error?        │
              └───┬───────┬───┘
                  │ No    │ Yes
                  ▼       ▼
          ┌───────────┐ ┌───────────┐
          │ End       │ │ Email     │
          │           │ │ Fail      │
          └───────────┘ └───────────┘
```

## Databricks Pipeline: pl_leaddiscovery_lead_propagation

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Breadcrumb               │
│ (Query Cosmos DB for         │
│  unprocessed notifications  │
│  - multiple types)           │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set Breadcrumb   │  │ Set Notification │
│ Variable         │  │ Type Variable    │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Breadcrumb and Notification │
│ Pipeline Return Value        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Wait                        │
│ (Synchronization point)     │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Log Workflow     │  │ Update          │
│ Start            │  │ Notification    │
│ (Cosmos DB)      │  │ In Progress     │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Wait II                      │
│ (Synchronization point)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Lookup Max/Min        │
│ Admit Days                   │
│ (Create lookup for admit     │
│  dates by hospital and       │
│  edipartnerfk)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get PA                       │
│ (Get patient accounts from   │
│  data ingestion publish)    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Set LeadProp Type            │
│ Variable (chc or nonchc)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: LeadProp Type      │
│ = chc?                       │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Process     │
│ Leads CHC   │    │ Leads       │
│             │    │ (Non-CHC)   │
└──────┬──────┘    └──────┬──────┘
       │                 │
       └────────┬────────┘
                │
                ▼
┌─────────────────────────────┐
│ Set ProcessLeads BC          │
│ Variable                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Non-CHC and       │
│ Process Leads Returned 1?    │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Check Leads │    │ Set        │
│             │    │ LeadProp    │
│             │    │ Push        │
│             │    │ Variable    │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ Set LeadProp Push            │
│ Variable                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: LeadProp Push      │
│ = 1?                        │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Push Leads  │    │ Decision:  │
│ (Sqoop Delta│    │ Sharding?   │
│  to SQL     │    │ (Inactive)  │
│  Server)    │    └─────────────┘
└──────┬──────┘
       │
       └──────────────────┐
                          │
                          ▼
              ┌───────────────────────┐
              │ Decision: Sharding?   │
              │ (Inactive)            │
              └───┬───────────────┬───┘
                  │ Yes            │ No
                  ▼                ▼
          ┌───────────────┐  ┌───────────────┐
          │ DB Sharding   │  │ Delete        │
          │ (Copy to ADLS)│  │ Trigger File  │
          └───────┬───────┘  └───────┬───────┘
                  │                  │
                  ▼                  │
          ┌───────────────┐          │
          │ DB Sharding    │          │
          │ Move           │          │
          └───────┬───────┘          │
                  │                  │
                  └────────┬─────────┘
                           │
                           ▼
              ┌───────────────────────┐
              │ Delete Trigger File   │
              └───────────┬───────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │ Update Notification   │
              │ Completed             │
              └───────────┬───────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │ End                   │
              └───────────────────────┘
```

## Key Process Steps

### Hadoop Workflow Steps:

1. **Check Previous Failed Workflow Status** - Check MapR DB for failed workflows
2. **Restart Previous Failed Workflow** - Attempt to restart failed workflows
3. **Get Notification** - Query MapR DB for lead propagation notifications
4. **Check Notification** - Shell script to verify notification exists
5. **Get Date** - Extract breadcrumb (date) from notification
6. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
7. **Get Min/Max Dates** - Create lookup tables for min/max admit dates by hospital and edipartnerfk
8. **Get PA** - Get patient accounts from data ingestion publish using PermID and GMRN data
9. **Process Leads** - Process leads with lookup data, lead status, and MapR DB mapping
10. **HDFS Dir Check** - Check if leads output directory exists
11. **Check Leads** - Check leads against previously sent leads and prepare for SQL export
12. **HDFS Dir Check Leads** - Check if sqoop output directory exists
13. **Sqoop Out** - Export to SQL Server table hdppatientacctxlead
14. **Sqoop Out HDPBatch** - Export to SQL Server table hdppatientacctxleadbatch
15. **Update Notification** - Update MapR DB notification status
16. **Log Notification** - Log notification to HDFS
17. **Email Notify** - Send success email
18. **Purge Intermediate Data** - Clean up intermediate data files
19. **Log Workflow Finish** - Log FINISHED status to MapR DB

### Databricks Pipeline Steps:

1. **Get Breadcrumb** - Query Cosmos DB runstatus table for unprocessed notifications (multiple notification types: ie_xref_lsb_propagation, es_xref_lsb_propagation, globalmrnmerge_xref_lsb_propagation, fc_xref_lsb_propagation, hfc_xref_lsb_propagation, ie_propagation_famc, es_propagation_famc, fc_propagation_famc, chc_xref_lsb_propagation)
2. **Set Breadcrumb Variable** - Extract and set breadcrumb from notification
3. **Set Notification Type Variable** - Extract and set notification type
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360
5. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running"
6. **Create Lookup Max/Min Admit Days** - Create lookup tables for min/max admit dates by hospital and edipartnerfk
7. **Get PA** - Get patient accounts from data ingestion publish using PermID and GMRN data
8. **Set LeadProp Type Variable** - Determine if processing CHC or non-CHC leads
9. **Process Leads** - Process leads with lookup data and lead status (conditional: CHC or non-CHC version)
10. **Set ProcessLeads BC Variable** - Set breadcrumb variable based on process leads output
11. **Check Leads** - Check leads against previously sent leads (only for non-CHC)
12. **Set LeadProp Push Variable** - Determine if leads should be pushed to SQL Server
13. **Push Leads** - Export to SQL Server using sqoop_delta notebook (hdppatientacctxlead and hdppatientacctxleadbatch)
14. **DB Sharding** - Copy data to ADLS (inactive/optional step)
15. **DB Sharding Move** - Move sharded data (inactive/optional step)
16. **Delete Trigger File** - Delete original trigger file
17. **Update Notification Completed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Sequential execution of all steps
- **Databricks**: Conditional execution based on notification type (CHC vs non-CHC) with different processing paths
- **Hadoop**: Uses Sqoop actions for SQL Server export
- **Databricks**: Uses sqoop_delta notebook for SQL Server export
- **Hadoop**: Single notification type processing
- **Databricks**: Supports multiple notification types in single pipeline
- **Hadoop**: Always runs check_leads step
- **Databricks**: Only runs check_leads for non-CHC leads
- **Hadoop**: Separate sqoop actions for two tables
- **Databricks**: Single sqoop_delta notebook handles both tables
- **Hadoop**: Uses MapR DB for notifications
- **Databricks**: Uses Cosmos DB for notifications
- **Databricks**: Includes optional sharding step (currently inactive)
- **Hadoop**: Includes purge intermediate data step
- **Databricks**: No explicit purge step (handled by delete trigger file)
