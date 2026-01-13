# Ops Hints Flow Diagrams

## Hadoop Workflow: process-hints

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get DateTime                │
│ (Shell script - extract     │
│  breadcrumb date)           │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Swap Proc   │    │ Email Fail  │
│ (Sqoop -    │    │             │
│  execute    │    └─────────────┘
│  uspEDIOPSSwap│
│  AndProcess │
│  Staging)   │
└──────┬──────┘
       │
       │ (continues even on error)
       │
       ▼
┌─────────────────────────────┐
│ Generate Hints               │
│ (Spark - generate OPS        │
│  payer/zip hints)           │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Sqoop Out   │    │ Email Fail  │
│ (Sqoop -    │    │             │
│  export to  │    └─────────────┘
│  SQL Server │
│  staging    │
│  table)     │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Swap Proc   │    │ Email Fail  │
│ Again       │    │             │
│ (Sqoop -    │    └─────────────┘
│  execute    │
│  stored     │
│  procedure  │
│  again)     │
└──────┬──────┘
       │
       │ (continues even on error)
       │
       ▼
┌─────────────────────────────┐
│ Email Notify Success        │
│ (Email notification)         │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ End                         │
└─────────────────────────────┘
```

## Databricks Pipeline: pl_hintsdiscovery_commercial

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ Get Breadcrumb   │    │ (Parallel)       │
│ (Databricks      │    │                  │
│  notebook -       │    │                  │
│  query Cosmos DB)│    │                  │
└──────┬───────────┘    └──────┬───────────┘
       │                       │
       ▼                       │
┌──────────────────┐          │
│ Set bc            │          │
│ (SetVariable)     │          │
└──────┬───────────┘          │
       │                       │
       ├───────────────────────┼──────────┐
       │                       │          │
       ▼                       ▼          ▼
┌──────────────────┐    ┌──────────────────┐
│ Pipeline BC      │    │ 360 Logger       │
│ Return Value     │    │ Running          │
│ (SetVariable)    │    │ (Databricks)    │
└──────┬───────────┘    └──────┬───────────┘
       │                       │
       └───────────┬───────────┘
                   │
                   ▼
          ┌─────────────────────────────┐
          │ Update Notification         │
          │ Inprogress                  │
          │ (Databricks)                │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Generate OPS Payerzip Hints │
          │ (Databricks notebook -     │
          │  generate OPS hints)        │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Commercial Hint Get          │
          │ MinMaxDT                    │
          │ (Databricks notebook -      │
          │  create admit date lookup   │
          │  by EDI partner FK)          │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Commercial Hints Get         │
          │ Candidate PatientAccts      │
          │ (Databricks notebook -       │
          │  get commercial hints       │
          │  candidates)                 │
          └──────┬──────────────────┘
                 │
                 ├──────────────────┐
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Append to   │    │ DB Sharding │
          │ patientacctx│    │ (If Condition│
          │ hints       │    │  - Inactive)│
          │ (If Condition│    │             │
          │  with       │    │             │
          │  append_hints│    │             │
          │  notebook)  │    │             │
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 └────────┬─────────┘
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
                 │ Completed                  │
                 │ (Databricks notebook)      │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ End                         │
                 └─────────────────────────────┘
```

## Key Process Steps

### Hadoop Workflow: process-hints

1. **Get DateTime** - Shell script to extract breadcrumb date
2. **Swap Proc** - Sqoop action to execute stored procedure `uspEDIOPSSwapAndProcessStaging` (swaps staging tables and processes data)
3. **Generate Hints** - Spark job (generate_ops_payerzip_hints.py) to generate OPS payer/zip hints:
   - Reads configuration tables (edipayers, edipartners, hospitals, etc.)
   - Creates OPS-enabled hospital and payer configurations
   - Reads patient accounts and filters for accounts >= 365 days old with charges > 0
   - Creates OPS zip hints (joins patient accounts with payer config on zip code)
   - Creates OPS lotto hints (joins patient accounts with lotto config on charges >= config charge)
   - Unions and writes pipe-delimited CSV to publish path
4. **Sqoop Out** - Sqoop action to export hints data to SQL Server staging table (OPS_Hadoop_Staging)
5. **Swap Proc Again** - Sqoop action to execute stored procedure `uspEDIOPSSwapAndProcessStaging` again (post-processing swap)
6. **Email Notify Success** - Send success email notification
7. **Email Fail** - Send failure email notification (on error)

### Databricks Pipeline: pl_hintsdiscovery_commercial

1. **Get Breadcrumb** - Databricks notebook to query Cosmos DB for notification type and retrieve breadcrumb date
2. **Set bc** - Set pipeline variable 'bc' with breadcrumb value
3. **Pipeline BC Return Value** - Set pipeline return value with breadcrumb (parallel with Set bc)
4. **360 Logger Running** - Log RUNNING status to Cosmos DB operations_log_360 (parallel with update notification)
5. **Update Notification Inprogress** - Update Cosmos DB runstatus table to "running" (parallel with log workflow start)
6. **Generate OPS Payerzip Hints** - Databricks notebook to generate OPS payer/zip hints (equivalent to Hadoop's generate-hints)
7. **Commercial Hint Get MinMaxDT** - Databricks notebook to create admit date lookup by EDI partner FK
8. **Commercial Hints Get Candidate PatientAccts** - Databricks notebook to get commercial hints candidates:
   - Reads OPS hints output and hintfoundcoverage
   - Joins with vsnappatientacctsflags
   - Maps payers to partners
   - Gets enabled EDI partners
   - Filters for commercial/Tricare partners
   - Gets candidate patient accounts with admit date filters
   - Filters by minimum charges per partner
   - Applies commercial filters (hospital attributes, insurance codes)
   - Filters out hints already in found coverage
   - Performs OHI batch processing
   - Selects best hints
   - Filters out hints already in patientacctsxhints and patientacctxleads
   - Writes final hints to scratch path
9. **Append to patientacctxhints** - If Condition: If hints were generated (output=1), append hints to patientacctsxhints table
10. **DB Sharding** - If Condition (Inactive): Database sharding step for hints export
11. **Delete Trigger File** - Databricks notebook to delete trigger file
12. **Update Notification Completed** - Update Cosmos DB runstatus table to "processed"
13. **End** - Pipeline completion

## Key Differences

1. **Stored Procedure Execution**: Hadoop uses Sqoop to execute stored procedure `uspEDIOPSSwapAndProcessStaging` before and after hints generation; Databricks doesn't have this step
2. **Breadcrumb Retrieval**: Hadoop uses shell script; Databricks queries Cosmos DB via Databricks notebook
3. **Hints Generation**: Both use Spark/Databricks for OPS hints generation, but Databricks uses notebook instead of Spark job
4. **SQL Server Export**: Hadoop uses Sqoop to export to SQL Server staging table; Databricks doesn't export to SQL Server (commercial hints are written to Delta Lake)
5. **Commercial Hints Processing**: Hadoop doesn't have commercial hints processing; Databricks has additional commercial hints generation after OPS hints
6. **Data Publishing**: Hadoop exports to SQL Server; Databricks writes to Delta Lake (patientacctsxhints)
7. **Logging**: Hadoop doesn't have explicit logging; Databricks uses 360 logger and Cosmos DB notifications
8. **Error Handling**: Hadoop has email notifications on failure; Databricks relies on ADF error handling
9. **Notification System**: Hadoop doesn't use notifications; Databricks uses Cosmos DB for notifications
10. **Post-Processing**: Hadoop executes stored procedure again after export; Databricks doesn't have this step
11. **Conditional Logic**: Hadoop executes steps even on error (swap-proc, swap-proc-again); Databricks uses If Condition for conditional hints appending
12. **Database Sharding**: Hadoop doesn't have sharding; Databricks has inactive sharding step
13. **Trigger File Management**: Hadoop doesn't have trigger files; Databricks deletes trigger file after processing
14. **Workflow Scope**: Hadoop only generates OPS hints; Databricks generates both OPS hints and commercial hints
