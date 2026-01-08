# Lead Discovery SubDOB Data Mining Flow Diagrams

## Hadoop Workflow: subdob :data mining

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
│ Log Workflow Start           │
│ (MapR DB oozie_360)         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Import EDI Subscriber        │
│ DOB Search                   │
│ (Import from SQL Server      │
│  EDISubscriberDOBSearch)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Lookup Family         │
│ (Create family lookup table) │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SubDOB Datamine EDI          │
│ (Mine DOB from EDI data)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SubDOB Datamine FC            │
│ CoverageID                   │
│ (Mine DOB from Family        │
│  Clustering coverage ID)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SubDOB Datamine Guarantor    │
│ (Mine DOB from guarantor     │
│  data)                       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SubDOB Datamine Family       │
│ (Mine DOB from family data)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SubDOB Datamine Unverified   │
│ (Mine DOB from unverified    │
│  data)                       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ LSB ID Lookup                │
│ (Create CPAXLSB cross-table) │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SubDOB Lookup LSB PermID    │
│ (Shell script - run          │
│  leadlookup by PermID)      │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SubDOB Datamine PermID       │
│ (Mine DOB from PermID        │
│  lookup results)             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ SubDOB Account Classify      │
│ (Classify accounts based on  │
│  mined DOB data)             │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Update Externally Sourced    │
│ Subscriber DOB               │
│ (Update SQL Server table)    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Create Sqoop Data            │
│ (Prepare data for SQL export)│
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ HDFS Dir Check               │
│ Externally Sourced           │
│ Subscriber DOB               │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Externally Sourced │
│ DOB Data Exists?            │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Out   │    │ HDFS Dir    │
│ Externally  │    │ Check EDI   │
│ Sourced     │    │ Subscriber  │
│ DOB Holding │    │ DOB Search  │
└──────┬──────┘    └──────┬──────┘
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ Sqoop Out Externally         │
│ Sourced DOB Batch            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ HDFS Dir Check EDI           │
│ Subscriber DOB Search        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: EDI Subscriber     │
│ DOB Search Data Exists?     │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Out   │    │ Log        │
│ EDI         │    │ Workflow    │
│ Subscriber  │    │ Finish      │
│ DOB Search  │    └──────┬──────┘
│ Holding     │           │
└──────┬──────┘           │
       │                  │
       ▼                  │
┌─────────────────────────────┐
│ Sqoop Out EDI Subscriber     │
│ DOB Search Batch             │
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
│ Purge Intermediate Data       │
│ (Clean up scratch files)    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ End                          │
└─────────────────────────────┘
```

## Databricks Pipeline: pl_leaddiscovery_subdob

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
└──────┬───────────┘  └──────┬──────────┘
       │                     │
       └──────────┬──────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Import EDI Subscriber        │
│ DOB Search                   │
│ (Import from SQL Server      │
│  EDISubscriberDOBSearch)     │
└──────────┬──────────────────┘
           │
           ├──────────────────────────────────────────────────────────────┐
           │                                                              │
           ▼                                                              ▼
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│ Create Lookup Family  │  │ SubDOB Datamine EDI  │  │ SubDOB Datamine FC   │
│ (Parallel)             │  │ (Parallel)           │  │ CoverageID           │
│                       │  │                      │  │ (Parallel)           │
└──────┬───────────────┘  └──────┬───────────────┘  └──────┬───────────────┘
       │                        │                        │
       │                        └────────┬───────────────┘
       │                                 │
       │                                 ▼
       │                    ┌──────────────────────┐
       │                    │ SubDOB Datamine       │
       │                    │ Guarantor             │
       │                    │ (Parallel)            │
       │                    └──────┬───────────────┘
       │                           │
       │                           ▼
       │                    ┌──────────────────────┐
       │                    │ SubDOB Datamine       │
       │                    │ Unverified            │
       │                    │ (Parallel)            │
       │                    └──────┬───────────────┘
       │                           │
       └───────────────────────────┼──────────────────────────┐
                                   │                          │
                                   ▼                          ▼
                      ┌──────────────────────┐  ┌──────────────────────┐
                      │ SubDOB Datamine      │  │ SubDOB Datamine      │
                      │ Family               │  │ Family               │
                      │ (Depends on Create   │  │ (Depends on Create   │
                      │  Lookup Family)      │  │  Lookup Family)     │
                      └──────┬───────────────┘  └──────┬───────────────┘
                             │                         │
                             └──────────┬──────────────┘
                                        │
                                        ▼
                      ┌─────────────────────────────┐
                      │ Create CPAXLSB XTable       │
                      │ (Create cross-table for     │
                      │  LSB ID lookup)             │
                      └──────────┬──────────────────┘
                                 │
                                 ▼
                      ┌─────────────────────────────┐
                      │ Run Leadlookup              │
                      │ (Notebook - lookup by       │
                      │  PermID against LSB)        │
                      └──────────┬──────────────────┘
                                 │
                                 ▼
                      ┌─────────────────────────────┐
                      │ SubDOB Datamine PermID       │
                      │ (Mine DOB from PermID        │
                      │  lookup results)             │
                      └──────────┬──────────────────┘
                                 │
                                 ▼
                      ┌─────────────────────────────┐
                      │ SubDOB Mine Account         │
                      │ Classify                    │
                      │ (Wait for all datamine      │
                      │  functions, then classify)  │
                      └──────────┬──────────────────┘
                                 │
                                 ├──────────────────┐
                                 │                  │
                                 ▼                  ▼
                      ┌──────────────────┐  ┌──────────────────┐
                      │ Update           │  │ Create Sqoop     │
                      │ Externally        │  │ Unvalidated       │
                      │ Sourced           │  │ SubDOB            │
                      │ Subscriber DOB    │  │                   │
                      └──────┬───────────┘  └──────┬───────────┘
                             │                     │
                             └──────────┬──────────┘
                                        │
                                        ▼
                      ┌─────────────────────────────┐
                      │ Sqoop Notebook               │
                      │ (Export both tables to       │
                      │  SQL Server via notebook)    │
                      └──────────┬──────────────────┘
                                 │
                                 ▼
                      ┌─────────────────────────────┐
                      │ Update Notification           │
                      │ Completed                    │
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
4. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
5. **Import EDI Subscriber DOB Search** - Import EDISubscriberDOBSearch table from SQL Server to HDFS
6. **Create Lookup Family** - Create family lookup table for DOB mining
7. **SubDOB Datamine EDI** - Mine subscriber DOB from EDI data
8. **SubDOB Datamine FC CoverageID** - Mine subscriber DOB from Family Clustering coverage ID data
9. **SubDOB Datamine Guarantor** - Mine subscriber DOB from guarantor data using PermID
10. **SubDOB Datamine Family** - Mine subscriber DOB from family data
11. **SubDOB Datamine Unverified** - Mine subscriber DOB from unverified data using PermID
12. **LSB ID Lookup** - Create CPAXLSB cross-table for Lead Service Base ID lookup
13. **SubDOB Lookup LSB PermID** - Run leadlookup shell script to lookup PermID against Lead Service Base
14. **SubDOB Datamine PermID** - Mine subscriber DOB from PermID lookup results
15. **SubDOB Account Classify** - Classify accounts based on all mined DOB data
16. **Update Externally Sourced Subscriber DOB** - Update ExternallySourcedSubscriberDOB table in SQL Server
17. **Create Sqoop Data** - Prepare data for SQL Server export
18. **HDFS Dir Check Externally Sourced Subscriber DOB** - Check if externally sourced DOB data exists
19. **Sqoop Out Externally Sourced Subscriber DOB Holding** - Export to SQL Server table
20. **Sqoop Out Externally Sourced Subscriber DOB Batch** - Export batch breadcrumb to SQL Server
21. **HDFS Dir Check EDI Subscriber DOB Search** - Check if EDI subscriber DOB search data exists
22. **Sqoop Out EDI Subscriber DOB Search Holding** - Export to SQL Server table
23. **Sqoop Out EDI Subscriber DOB Search Batch** - Export batch breadcrumb to SQL Server
24. **Log Workflow Finish** - Log FINISHED status to MapR DB
25. **Email Notify** - Send success email
26. **Purge Intermediate Data** - Clean up scratch data files

### Databricks Pipeline Steps:

1. **Get Breadcrumb** - Query Cosmos DB for unprocessed notifications (using get_bc notebook)
2. **Set Breadcrumb Variable** - Extract and set breadcrumb from notification
3. **Breadcrumb Pipeline Return Value** - Set pipeline return value with breadcrumb
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360
5. **Update Notification In Progress** - Update Cosmos DB runstatus table to "running" (using log_notification notebook)
6. **Import EDI Subscriber DOB Search** - Import EDISubscriberDOBSearch table from SQL Server to ADLS
7. **Create Lookup Family** - Create family lookup table (parallel with datamine functions)
8. **SubDOB Datamine EDI** - Mine subscriber DOB from EDI data (parallel execution)
9. **SubDOB Datamine FC CoverageID** - Mine subscriber DOB from Family Clustering coverage ID data (parallel execution)
10. **SubDOB Datamine Guarantor** - Mine subscriber DOB from guarantor data using PermID (parallel execution)
11. **SubDOB Datamine Unverified** - Mine subscriber DOB from unverified data using PermID (parallel execution)
12. **SubDOB Datamine Family** - Mine subscriber DOB from family data (depends on create lookup family)
13. **Create CPAXLSB XTable** - Create cross-table for Lead Service Base ID lookup
14. **Run Leadlookup** - Lookup PermID against Lead Service Base using notebook (subdob_LeadLookupByID)
15. **SubDOB Datamine PermID** - Mine subscriber DOB from PermID lookup results
16. **SubDOB Mine Account Classify** - Classify accounts based on all mined DOB data (waits for all datamine functions: EDI, FC CoverageID, Guarantor, PermID, Family)
17. **Update Externally Sourced Subscriber DOB** - Update ExternallySourcedSubscriberDOB table in SQL Server
18. **Create Sqoop Unvalidated SubDOB** - Prepare unvalidated data for SQL Server export
19. **Sqoop Notebook** - Export both tables (ExternallySourcedSubscriberDOB and EDISubscriberDOBSearch) to SQL Server via notebook
20. **Update Notification Completed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

- **Hadoop**: Sequential execution of all datamine functions
- **Databricks**: Parallel execution of datamine functions (EDI, FC CoverageID, Guarantor, Unverified) after import, with Family depending on Create Lookup Family
- **Hadoop**: Uses shell script (run_leadlookup.sh) for PermID lookup
- **Databricks**: Uses Databricks notebook (subdob_LeadLookupByID) for PermID lookup
- **Hadoop**: Uses Sqoop actions for SQL Server export (separate actions for each table and batch)
- **Databricks**: Uses single sqoop notebook (subdob_sqoop_out) to handle both tables
- **Hadoop**: Conditional sqoop export based on HDFS directory checks
- **Databricks**: Single sqoop notebook handles all exports
- **Hadoop**: Uses MapR DB for logging
- **Databricks**: Uses Cosmos DB for logging and notifications
- **Hadoop**: Includes purge intermediate data step
- **Databricks**: No explicit purge step
- **Hadoop**: Separate HDFS directory checks for each table before sqoop export
- **Databricks**: No directory checks, sqoop notebook handles all exports
- **Hadoop**: Account classify runs after all datamine functions sequentially
- **Databricks**: Account classify waits for all parallel datamine functions to complete
- **Hadoop**: Uses get_datetime.sh shell script for breadcrumb
- **Databricks**: Uses get_bc notebook for breadcrumb
