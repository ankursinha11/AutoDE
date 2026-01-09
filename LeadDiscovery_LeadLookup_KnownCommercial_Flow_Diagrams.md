# Lead Discovery Lead Lookup Known Commercial Flow Diagrams

## Hadoop Workflow: leaddiscovery: leadlookup: knowncommercial

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
│ Check Notification           │
│ (Shell script)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Notification       │
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
│ Get Min Max Dates            │
│ (Create lookup for min/max   │
│  admit dates by hospital     │
│  and EDI partner)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Candidate Patient       │
│ Accounts                     │
│ (Get patient accounts from   │
│  known commercial data)      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Process CPA LSB XTable       │
│ (Create cross-table for      │
│  candidate patient accounts  │
│  and LSB helper)            │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Process          │  │ Process          │
│ GlobalMRNIFK     │  │ PermID           │
│ Leadlookup       │  │ Leadlookup       │
│ (Shell script)   │  │ (Shell script)   │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Error Check                 │
└──────────┬──────────────────┘
           │
           ├──────────────────────────────────┐
           │                                  │
           ▼                                  ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Process          │  │ Process          │  │ Process          │
│ SSN              │  │ Medical Rec Num  │  │ Clustered Acct   │
│ Leadlookup       │  │ Leadlookup       │  │ FK Leadlookup    │
│ (Shell script)   │  │ (Shell script)    │  │ (Shell script)   │
└──────┬───────────┘  └──────┬───────────┘  └──────┬───────────┘
       │                     │                     │
       └──────────┬──────────┴─────────────────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Error Check                 │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge CPA X Leads            │
│ (Merge candidate patient     │
│  accounts with leads)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Process Leads                │
│ (Process and enrich leads    │
│  with patient account data)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ HDFS Dir Check               │
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
│ Check Leads │    │ Update      │
│             │    │ Notification│
└──────┬──────┘    └──────┬──────┘
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ HDFS Dir Check Leads         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Sqoop Data         │
│ Exists?                      │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Out   │    │ Update      │
│ (to         │    │ Notification│
│  hdppatient │    └──────┬──────┘
│  acctxlead) │           │
└──────┬──────┘           │
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ Sqoop Out HDPBatch          │
│ (to hdppatientacctxleadbatch)│
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Source is          │
│ globalmrn_assign?            │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Update      │    │ Update      │
│ Notification│    │ Notification│
│ (Special)   │    │ (Regular)   │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └──────────┬───────┘
                  │
                  ├──────────────────────────────────────┐
                  │                                    │
                  ▼                                    ▼
      ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
      │ Log Notification │  │ Email Notify    │  │ Purge            │
      │                  │  │                 │  │ Intermediate     │
      │                  │  │                 │  │ Data             │
      └──────┬───────────┘  └──────┬───────────┘  └──────┬───────────┘
             │                     │                     │
             └──────────┬──────────┴─────────────────────┘
                        │
                        ▼
      ┌─────────────────────────────┐
      │ Log Workflow Finish         │
      │ (MapR DB oozie_360)        │
      └──────────┬──────────────────┘
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

## Databricks Pipeline: pl_leaddiscovery_leadlookup_knowncommercial

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
└──────┬───────────┘  └──────┬──────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Knowncommercial Get         │
│ Min Max Dates               │
│ (Create lookup for min/max   │
│  admit dates by hospital     │
│  and EDI partner)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Knowncommercial Get          │
│ Candidate Patient Accounts   │
│ (Get patient accounts from   │
│  known commercial data)      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Knowncommercial Process      │
│ CPA LSB XTable              │
│ (Create cross-table for      │
│  candidate patient accounts  │
│  and LSB helper)            │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Process          │  │ Process          │
│ GlobalMRNIFK     │  │ PermID           │
│ Leadlookup       │  │ Leadlookup       │
│ (Notebook)       │  │ (Notebook)       │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Wait1                        │
│ (Synchronization)            │
└──────────┬──────────────────┘
           │
           ├──────────────────────────────────┐
           │                                  │
           ▼                                  ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Process          │  │ Process          │  │ Process          │
│ SSN              │  │ Medical Rec Num  │  │ Clustered Acct   │
│ Leadlookup       │  │ Leadlookup       │  │ FK Leadlookup    │
│ (Notebook)       │  │ (Notebook)       │  │ (Notebook)       │
└──────┬───────────┘  └──────┬───────────┘  └──────┬───────────┘
       │                     │                     │
       └──────────┬──────────┴─────────────────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Wait1_copy1                 │
│ (Synchronization)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Merge CPA X Leads            │
│ (Merge candidate patient     │
│  accounts with leads)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Process Leads                │
│ (Process and enrich leads    │
│  with patient account data,  │
│  return count)               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check Whether Process Lead   │
│ Output Exist or Not          │
│ (IfCondition: output == 1?) │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Check Leads │    │ Set         │
│             │    │ check_leads  │
│             │    │ ret_val = 0  │
└──────┬──────┘    └──────┬──────┘
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ Set check_leads_ret_val     │
│ (From check_leads output)    │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Check Lead       │  │ Check Lead       │
│ Exist Sharding   │  │ Exist            │
│ (IfCondition:    │  │ (IfCondition:    │
│  ret_val == 1)   │  │  ret_val == 1)   │
│ (Inactive)       │  │                  │
└──────┬───────────┘  └──────┬───────────┘
       │                     │
       │                     ▼
       │            ┌──────────────────┐
       │            │ Sqoop Out        │
       │            │ (Notebook)       │
       │            └──────┬───────────┘
       │                   │
       └──────────┬────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Delete Trigger File          │
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
3. **Get Notification** - Get notification from MapR DB runstatus table for knowncommercial
4. **Check Notification** - Shell script to check if notification file exists in HDFS
5. **Get Date** - Extract breadcrumb (date) from notification
6. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
7. **Get Min Max Dates** - Create lookup tables for min/max admit dates by hospital and EDI partner
8. **Get Candidate Patient Accounts** - Get patient accounts from known commercial published data
9. **Process CPA LSB XTable** - Create cross-table for candidate patient accounts and Lead Service Base helper
10. **Process GlobalMRNIFK Leadlookup** - Shell script to lookup leads by GlobalMRNIFK (parallel with PermID)
11. **Process PermID Leadlookup** - Shell script to lookup leads by PermID (parallel with GlobalMRNIFK)
12. **Process SSN Leadlookup** - Shell script to lookup leads by SSN (parallel with Medical Rec Num and Clustered Acct FK)
13. **Process Medical Rec Num Leadlookup** - Shell script to lookup leads by Medical Record Number (parallel)
14. **Process Clustered Acct FK Leadlookup** - Shell script to lookup leads by Clustered Account FK (parallel)
15. **Merge CPA X Leads** - Merge candidate patient accounts with leads from all lookups
16. **Process Leads** - Process and enrich leads with patient account data, demographics, and lead status
17. **HDFS Dir Check** - Check if leads output directory exists
18. **Check Leads** - Check leads against sent leads, prepare sqoop data
19. **HDFS Dir Check Leads** - Check if sqoop output directory exists
20. **Sqoop Out** - Export to SQL Server table hdppatientacctxlead
21. **Sqoop Out HDPBatch** - Export batch breadcrumb to SQL Server table hdppatientacctxleadbatch
22. **Update Notification** - Update MapR DB runstatus table (special handling for globalmrn_assign source)
23. **Log Notification** - Log notification to HDFS notification directory
24. **Email Notify** - Send success email
25. **Purge Intermediate Data** - Clean up scratch data files
26. **Log Workflow Finish** - Log FINISHED status to MapR DB

### Databricks Pipeline Steps:

1. **Get Breadcrumb** - Query Cosmos DB for unprocessed notifications with notificationtype="knowncommercial" (using get_breadcrumb notebook)
2. **Set Breadcrumb Variable** - Extract and set breadcrumb from notification
3. **Breadcrumb Pipeline Return Value** - Set pipeline return value with breadcrumb
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360 (parallel with update notification)
5. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running" (parallel with log workflow start)
6. **Knowncommercial Get Min Max Dates** - Create lookup tables for min/max admit dates by hospital and EDI partner
7. **Knowncommercial Get Candidate Patient Accounts** - Get patient accounts from known commercial published data
8. **Knowncommercial Process CPA LSB XTable** - Create cross-table for candidate patient accounts and Lead Service Base helper
9. **Process GlobalMRNIFK Leadlookup** - Lookup leads by GlobalMRNIFK using notebook (parallel with PermID)
10. **Process PermID Leadlookup** - Lookup leads by PermID using notebook (parallel with GlobalMRNIFK)
11. **Wait1** - Synchronization point after first set of parallel lookups
12. **Process SSN Leadlookup** - Lookup leads by SSN using notebook (parallel with Medical Rec Num and Clustered Acct FK)
13. **Process Medical Rec Num Leadlookup** - Lookup leads by Medical Record Number using notebook (parallel)
14. **Process Clustered Acct FK Leadlookup** - Lookup leads by Clustered Account FK using notebook (parallel)
15. **Wait1_copy1** - Synchronization point after second set of parallel lookups
16. **Merge CPA X Leads** - Merge candidate patient accounts with leads from all lookups
17. **Process Leads** - Process and enrich leads with patient account data, demographics, and lead status, return count
18. **Check Whether Process Lead Output Exist or Not** - IfCondition checking if process_leads output == 1
19. **Check Leads** - If condition true, check leads against sent leads, prepare sqoop data
20. **Set check_leads_ret_val** - Set variable from check_leads output
21. **Check Lead Exist** - IfCondition checking if check_leads_ret_val == 1
22. **Sqoop Out** - If condition true, export to SQL Server using notebook (sqoop_out)
23. **Check Lead Exist Sharding** - IfCondition for sharding (currently inactive)
24. **Delete Trigger File** - Delete ADLS trigger file that started this pipeline
25. **Update Notification Completed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Uses MapR DB for notifications and logging
- **Databricks**: Uses Cosmos DB for notifications and logging
- **Hadoop**: Gets notification from MapR DB and checks HDFS notification file
- **Databricks**: Gets breadcrumb from Cosmos DB runstatus table directly
- **Hadoop**: Uses shell scripts (run_leadlookup.sh) for all leadlookup operations
- **Databricks**: Uses Databricks notebooks (knowncommercial_run_leadlookup) for all leadlookup operations
- **Hadoop**: Uses fork/join for parallel execution of leadlookups
- **Databricks**: Uses Wait activities for synchronization between parallel leadlookups
- **Hadoop**: Conditional sqoop export based on HDFS directory checks
- **Databricks**: Conditional sqoop export based on process_leads and check_leads return values
- **Hadoop**: Separate sqoop actions for main table and batch table
- **Databricks**: Single sqoop notebook handles both tables
- **Hadoop**: Includes special handling for globalmrn_assign source in update notification
- **Databricks**: No special handling for different sources
- **Hadoop**: Includes email notification and purge intermediate data
- **Databricks**: No email notification, no explicit purge step
- **Hadoop**: Logs notification to HDFS notification directory
- **Databricks**: Deletes trigger file from ADLS
- **Hadoop**: Uses get_datetime.sh shell script for breadcrumb
- **Databricks**: Uses get_breadcrumb notebook for breadcrumb
- **Hadoop**: Includes error decision nodes after fork/join operations
- **Databricks**: Uses Wait activities for synchronization without explicit error checks
- **Databricks**: Includes optional sharding step (currently inactive)
