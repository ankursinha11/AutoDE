# Lead Discovery Lead Verify Flow Diagrams

## Hadoop Workflow: leaddiscovery: leadverify

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
│ Get Candidate Patient        │
│ Accounts                     │
│ (From selfpay or KC          │
│  patient accounts)           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check Valid Candidates       │
│ (Apply leadverify checks     │
│  and filters)                │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Leadverify Lookup Leads      │
│ (Shell script - run          │
│  leadverify lookup)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Filter LSB Leads             │
│ (Filter leads against        │
│  Lead Service Base)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Filter Leadverify Repo       │
│ Leads                        │
│ (Filter leads against        │
│  leadverify repository)      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Process Leads                │
│ (Process leads with lookup   │
│  data and lead status)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check Leads                  │
│ (Check leads against         │
│  previously sent leads)      │
└──────────┬──────────────────┘
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
│ Sqoop Out   │    │ Update      │
│ (Export to  │    │ Notification│
│  SQL Server │    └──────┬──────┘
│  hdppatient │           │
│  acctxlead) │           │
└──────┬──────┘           │
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ Sqoop Out HDPBatch           │
│ (Export to SQL Server        │
│  hdppatientacctxleadbatch)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Notification          │
│ (Update MapR DB)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Purge Scratch Data           │
│ (Clean up scratch files)    │
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
│ Log         │  │ Email       │  │ Log         │
│ Notification│  │ Notify       │  │ Workflow    │
│             │  │             │  │ Finish      │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                 │
       └────────────────┼─────────────────┘
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

## Databricks Pipeline: pl_leaddiscovery_leadverify

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
│  - pa_lead_verification)    │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set Breadcrumb   │  │ Breadcrumb       │
│ Variable         │  │ Pipeline Return  │
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
└──────┬───────────┘  └──────┬──────────┘
       │                     │
       └──────────┬──────────┘
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
│ Get Candidates               │
│ (Get candidate patient       │
│  accounts from selfpay)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Apply Leadverify Checks      │
│ (Apply leadverify checks     │
│  and filters)                │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Lookup Leads                 │
│ (Lookup leads using          │
│  Leadverify notebook)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Filter LSB Leads             │
│ (Filter leads against        │
│  Lead Service Base)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Filter Leadverify Repo       │
│ Leads                        │
│ (Filter leads against        │
│  leadverify repository)      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Process Leads                │
│ (Process leads with lookup   │
│  data and lead status)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Process Leads      │
│ Returned 1?                 │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Check Leads │    │ Set Check   │
│             │    │ Leads Ret   │
│             │    │ Val = 0     │
└──────┬──────┘    └──────┬──────┘
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ Set Check Leads Ret Val      │
│ Variable                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Check Leads        │
│ Ret Val = 1?                │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Out   │    │ Delete      │
│ (Export to  │    │ Trigger     │
│  SQL Server │    │ File        │
│  via        │    │             │
│  notebook)  │    │             │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └──────────┬───────┘
                  │
                  ▼
      ┌───────────────────────┐
      │ Decision: Sharding?    │
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
      │ Update Notification    │
      │ Processed              │
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
3. **Get Notification** - Query MapR DB for leadverify notifications
4. **Check Notification** - Shell script to verify notification exists
5. **Get Date** - Extract breadcrumb (date) from notification
6. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
7. **Get Min/Max Dates** - Create lookup tables for min/max admit dates by hospital and edipartnerfk
8. **Get Candidate Patient Accounts** - Get candidate patient accounts from selfpay or KC patient accounts using PermID data
9. **Check Valid Candidates** - Apply leadverify checks and filters to validate candidates
10. **Leadverify Lookup Leads** - Run leadverify lookup using shell script (run_leadverify.sh) against Lead Service Base
11. **Filter LSB Leads** - Filter leads against Lead Service Base to remove duplicates
12. **Filter Leadverify Repo Leads** - Filter leads against leadverify repository to remove previously processed leads
13. **Process Leads** - Process leads with lookup data, admit date lookup, and lead status
14. **Check Leads** - Check leads against previously sent leads in leadlookup repository
15. **HDFS Dir Check Leads** - Check if sqoop output directory exists
16. **Sqoop Out** - Export to SQL Server table hdppatientacctxlead using Sqoop action
17. **Sqoop Out HDPBatch** - Export to SQL Server table hdppatientacctxleadbatch using Sqoop action
18. **Update Notification** - Update MapR DB notification status
19. **Purge Scratch Data** - Clean up scratch data files
20. **Log Notification** - Log notification to HDFS
21. **Email Notify** - Send success email
22. **Log Workflow Finish** - Log FINISHED status to MapR DB

### Databricks Pipeline Steps:

1. **Get Breadcrumb** - Query Cosmos DB runstatus table for unprocessed notifications (pa_lead_verification)
2. **Set Breadcrumb Variable** - Extract and set breadcrumb from notification
3. **Breadcrumb Pipeline Return Value** - Set pipeline return value with breadcrumb
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360
5. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running"
6. **Get Min/Max Dates** - Create lookup tables for min/max admit dates by hospital and edipartnerfk
7. **Get Candidates** - Get candidate patient accounts from selfpay patient accounts using PermID and GMRN data
8. **Apply Leadverify Checks** - Apply leadverify checks and filters to validate candidates
9. **Lookup Leads** - Lookup leads using Leadverify notebook (Leadverify_LeadLookupByID) against Lead Service Base
10. **Filter LSB Leads** - Filter leads against Lead Service Base to remove duplicates
11. **Filter Leadverify Repo Leads** - Filter leads against leadverify repository to remove previously processed leads
12. **Process Leads** - Process leads with lookup data, admit date lookup, and lead status
13. **Decision: Process Leads Returned 1?** - Check if process leads returned 1 (leads found)
14. **Check Leads** - Check leads against previously sent leads (only if process leads returned 1)
15. **Set Check Leads Ret Val Variable** - Set return value from check leads
16. **Decision: Check Leads Ret Val = 1?** - Check if check leads returned 1 (leads to export)
17. **Sqoop Out** - Export to SQL Server using sqoop_out notebook (hdppatientacctxlead and hdppatientacctxleadbatch)
18. **Decision: Sharding?** - Optional sharding step (currently inactive)
19. **DB Sharding** - Copy data to ADLS (inactive/optional step)
20. **DB Sharding Move** - Move sharded data (inactive/optional step)
21. **Delete Trigger File** - Delete original trigger file
22. **Update Notification Processed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Sequential execution of all steps
- **Databricks**: Conditional execution based on process leads and check leads return values
- **Hadoop**: Uses shell script (run_leadverify.sh) for leadverify lookup
- **Databricks**: Uses Databricks notebook (Leadverify_LeadLookupByID) for leadverify lookup
- **Hadoop**: Uses Sqoop actions for SQL Server export
- **Databricks**: Uses sqoop_out notebook for SQL Server export
- **Hadoop**: Always runs check_leads step
- **Databricks**: Only runs check_leads if process_leads returned 1
- **Hadoop**: Always runs sqoop export if check_leads output exists
- **Databricks**: Only runs sqoop export if check_leads returned 1
- **Hadoop**: Separate sqoop actions for two tables
- **Databricks**: Single sqoop_out notebook handles both tables
- **Hadoop**: Uses MapR DB for notifications
- **Databricks**: Uses Cosmos DB for notifications
- **Hadoop**: Includes purge scratch data step
- **Databricks**: No explicit purge step (handled by delete trigger file)
- **Databricks**: Includes optional sharding step (currently inactive)
- **Hadoop**: Has separate action for getting KC candidates (leadverify_get_candidate_from_kc_patientaccts) - not always used
- **Databricks**: Single get_candidates notebook handles all candidate types
