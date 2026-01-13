# Lead Service Base CHC Flow Diagrams

## Hadoop Workflow: leadservicebase : lead gen and update : chc

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check Previous WF Status    │
│ (Spark - MapR DB)           │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Restart     │    │ Email Fail  │
│ Previous    │    │             │
│ Failed WF   │    └─────────────┘
│ (Shell)     │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Decision: Restart            │
│ Successful?                 │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get Kafka   │    │ End         │
│ Notification│    └─────────────┘
│ (Shell)     │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Decision:   │    │ Email Fail  │
│ Notification│    │             │
│ Exists?     │    └─────────────┘
└───┬────┬────┘
    │    │
    │ Yes│ No
    ▼    ▼
┌─────────────┐    ┌─────────────┐
│ 360 Log     │    │ End         │
│ Start       │    └─────────────┘
│ (Spark)     │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ CHC Process │    │ Email Fail  │
│ PreLSB Data │    │             │
│ (Spark -    │    └─────────────┘
│  chc_populate│
│  _leads.py) │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Email Fail  │
│ GMRN Insert │    │             │
│ (Shell -    │    └─────────────┘
│  chc_run_   │
│  leadservice│
│  base.sh)   │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Email Fail  │
│ PermID      │    │             │
│ Insert      │    └─────────────┘
│ (Shell -    │
│  chc_run_   │
│  leadservice│
│  base.sh)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Fork: Email Notify Success   │
│ (5 parallel paths)          │
└───┬──────────────────────┬───┘
    │                      │
    ▼                      ▼
┌─────────────┐    ┌─────────────┐
│ 360 Log     │    │ Log         │
│ Finish      │    │ Notification│
│ (Spark)     │    │ (Shell)     │
└──────┬──────┘    └──────┬──────┘
       │                  │
       │    ┌─────────────┼─────────────┐
       │    │             │             │
       ▼    ▼             ▼             ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Update      │    │ Create      │    │ Email       │
│ Notification│    │ Propagation │    │ Notify      │
│ (Spark)     │    │ Notification│    │             │
│             │    │ (Spark)     │    │             │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
                          ▼
                 ┌─────────────────────────────┐
                 │ Join: Email Success End     │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ Decision: Any Errors?       │
                 └───┬────────────────────┬────┘
                     │ No                 │ Yes
                     ▼                    ▼
              ┌─────────────┐    ┌─────────────┐
              │ End         │    │ Fork:       │
              └─────────────┘    │ Email Fail  │
                                 │ (Parallel)  │
                                 └───┬────┬────┘
                                     │    │
                                     ▼    ▼
                              ┌─────────────┐    ┌─────────────┐
                              │ 360 Log     │    │ Email Fail  │
                              │ Fail        │    │ Sent        │
                              │ (Spark)     │    │             │
                              └──────┬──────┘    └──────┬──────┘
                                     │                  │
                                     └────────┬─────────┘
                                              │
                                              ▼
                                     ┌─────────────────────────────┐
                                     │ Join: Email Fail End       │
                                     └──────┬──────────────────┘
                                            │
                                            ▼
                                     ┌─────────────────────────────┐
                                     │ End                         │
                                     └─────────────────────────────┘
```

## Databricks Pipeline: pl_leadservicebase_chc

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ Set Basepath     │    │ Set Breadcrumb   │
│ (SetVariable)    │    │ (SetVariable)    │
└──────┬───────────┘    └──────┬───────────┘
       │                       │
       │                       │
       ▼                       ▼
┌──────────────────┐    ┌──────────────────┐
│ Set              │    │ (Parallel)       │
│ Notification Type│    │                  │
│ (SetVariable)    │    │                  │
└──────┬───────────┘    └──────┬───────────┘
       │                       │
       └───────────┬───────────┘
                   │
                   ▼
          ┌─────────────────────────────┐
          │ CHC Populate Leads           │
          │ (Databricks notebook -       │
          │  process CHC data and        │
          │  create lead demographics)   │
          └──────┬──────────────────┘
                 │
                 ├──────────────────┐
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Process     │    │ (Error      │
          │ GMRN Insert │    │  handling)  │
          │ CHC         │    │             │
          │ (Databricks │    │             │
          │  notebook - │    │             │
          │  populate   │    │             │
          │  GMRN       │    │             │
          │  references)│    │             │
          └──────┬──────┘    └─────────────┘
                 │
                 ├──────────────────┐
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Process     │    │ (Error      │
          │ PermID      │    │  handling)  │
          │ Insert CHC  │    │             │
          │ (Databricks │    │             │
          │  notebook - │    │             │
          │  populate   │    │             │
          │  PermID     │    │             │
          │  references)│    │             │
          └──────┬──────┘    └─────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Log Notification            │
          │ Leadpropagation CHC         │
          │ (Databricks notebook -      │
          │  create notification for    │
          │  downstream workflow)       │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Create Trigger File         │
          │ Leadpropagation CHC         │
          │ (Databricks notebook -      │
          │  create trigger file for     │
          │  lead propagation)           │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ End                         │
          └─────────────────────────────┘
```

## Key Process Steps

### Hadoop Workflow: leadservicebase : lead gen and update : chc

1. **Check Previous WF Status** - Spark job to check for previous failed workflow runs
2. **Restart Previous Failed WF** - Shell script to restart failed workflow if found
3. **Decision: Restart Successful?** - If restart successful, continue; else end
4. **Get Kafka Notification** - Shell script to get notification from Kafka/MapR DB for CHC processing
5. **Decision: Notification Exists?** - If notification exists, continue; else end
6. **360 Log Start** - Spark job to log workflow start to MapR DB
7. **CHC Process PreLSB Data** - Spark job (chc_populate_leads.py) to process CHC data and create lead demographics
8. **Process GMRN Insert** - Shell script (chc_run_leadservicebase.sh) to populate GMRN references in Lead Service Base
9. **Process PermID Insert** - Shell script (chc_run_leadservicebase.sh) to populate PermID references in Lead Service Base
10. **Fork: Email Notify Success** - Parallel execution of 5 paths:
    - 360 Log Finish (Spark)
    - Log Notification (Shell)
    - Update Notification (Spark)
    - Create Propagation Notification (Spark)
    - Email Notify
11. **Join: Email Success End** - Wait for all parallel paths
12. **Decision: Any Errors?** - Check if any errors occurred
13. **Fork: Email Fail** - Parallel execution of error logging and email (if errors)
14. **360 Log Fail** - Spark job to log failure to MapR DB
15. **Email Fail Sent** - Send failure email notification
16. **Join: Email Fail End** - Wait for both parallel paths
17. **End** - Workflow completion

### Databricks Pipeline: pl_leadservicebase_chc

1. **Set Basepath** - Set pipeline variable with base path
2. **Set Breadcrumb** - Set pipeline variable 'dt' with breadcrumb value (from pipeline parameter)
3. **Set Notification Type** - Set pipeline variable with notification type (from pipeline parameter)
4. **CHC Populate Leads** - Databricks notebook to process CHC data and create lead demographics
5. **Process GMRN Insert CHC** - Databricks notebook to populate GMRN references in Lead Service Base
6. **Process PermID Insert CHC** - Databricks notebook to populate PermID references in Lead Service Base
7. **Log Notification Leadpropagation CHC** - Databricks notebook to create notification for downstream lead propagation workflow
8. **Create Trigger File Leadpropagation CHC** - Databricks notebook to create trigger file for lead propagation
9. **End** - Pipeline completion

## Key Differences

1. **Notification System**: Hadoop uses Kafka/MapR DB with shell scripts; Databricks receives notification type as pipeline parameter
2. **Breadcrumb Retrieval**: Hadoop gets breadcrumb from Kafka notification; Databricks receives breadcrumb as pipeline parameter
3. **Workflow Starter Check**: Hadoop checks and restarts previous failed workflows; Databricks relies on ADF error handling
4. **Data Processing**: Hadoop uses Spark job for populate leads; Databricks uses Databricks notebook
5. **Lead Service Base Population**: Hadoop uses shell scripts (chc_run_leadservicebase.sh); Databricks uses Databricks notebooks (PopulateLsbLeadsReference)
6. **Logging**: Hadoop uses Spark jobs to log to MapR DB (360 logger); Databricks doesn't have explicit logging steps in this pipeline
7. **Error Handling**: Hadoop has email notifications on failure; Databricks relies on ADF error handling
8. **Parallel Execution**: Hadoop uses fork/join for parallel logging and notifications; Databricks executes sequentially
9. **Notification Logging**: Hadoop uses shell script and Spark job; Databricks uses Databricks notebook
10. **Propagation Notification**: Hadoop creates propagation notification via Spark job; Databricks creates trigger file via Databricks notebook
11. **Email Notifications**: Hadoop sends email on success/failure; Databricks doesn't have email notifications
12. **Parameter Passing**: Hadoop gets parameters from notification; Databricks receives parameters directly from calling pipeline
13. **360 Logging**: Hadoop has explicit 360 logging at start, finish, and failure; Databricks doesn't have 360 logging in this pipeline
