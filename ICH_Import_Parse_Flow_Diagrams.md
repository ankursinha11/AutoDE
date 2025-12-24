# ICH Import Parse - Flow Diagrams

## Pipeline Comparison
- **Hadoop**: `leadrepository : ich_import_parse` workflow
- **Databricks**: `pl_leadrepo_escan_ich_import` pipeline
- **Mapping**: One-to-one mapping

---

## 🔄 HADOOP WORKFLOW: leadrepository : ich_import_parse

```
┌─────────────────────────────────────────────────────────────────┐
│                    START                                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  check_previous_wf_status                                       │
│  (Spark) - Check for previous failed workflows                   │
│  Script: oozie_360_wf_checker.py                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  restart_previous_failed_wf                                      │
│  (Shell) - Restart failed workflows if needed                    │
│  Script: oozie_360_wf_runner_v1.sh                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  oozie_runner_decision                                           │
│  (Decision) - Check if restart was needed                        │
│  If output='true' → get_notification                            │
│  If output='false' → end                                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                    ┌────┴────┐
                    │  true   │
                    └────┬────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  get_notification                                                │
│  (Spark) - Get notification from MapR DB                         │
│  Script: get_notification.py                                    │
│  Table: runstatus (notificationtype='ie_prebdf')                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  check_notification                                              │
│  (Shell) - Check notification file in HDFS                       │
│  Script: check_notification_ingest.sh                           │
│  Output: startjob, date, source                                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  decisionnode                                                    │
│  (Decision) - Check if job should start                          │
│  If startjob='true' → get-date                                  │
│  If startjob='false' → end                                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                    ┌────┴────┐
                    │  true   │
                    └────┬────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  get-date                                                        │
│  (Shell) - Extract business date (breadcrumb)                   │
│  Script: get_datetime.sh                                        │
│  Input: date from check_notification                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  oozie_360_log_start                                            │
│  (Spark) - Log workflow start to MapR DB                        │
│  Script: oozie_360_logger_v1.py                                 │
│  Table: maprdb_oozie_360                                        │
│  Status: RUNNING                                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  lr_ich_extract_trans_demo                                       │
│  (Pig) - Extract transaction data from XML files                 │
│  Script: lr_ich_extract_transaction_demo.pig                     │
│  Input: ${ie_input_dir}/{date} (XML files)                      │
│  Output: ${ie_stage_op_trans_path}/{date} (delimited, \001)     │
│  Logic:                                                          │
│    - Parse XML using XPathAll                                    │
│    - Extract subscriber (HL01='3') and dependent (HL01='4')   │
│    - Filter: responsestatus != '3' when responseresultext empty│
│    - Filter: non-empty transactionkey                           │
│    - Apply business rules (groupnumber, coverageid selection)   │
│    - Filter: non-empty coverageid                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  lr_ich_extract_cooked_trans_demo                               │
│  (Spark) - Process and partition transaction data               │
│  Script: lr_ich_range_partitioner.py                            │
│  Input: ${ie_stage_op_trans_path}/{date}                         │
│  Output: ${ie_cooked_op_trans_path} (partitioned by transactionrange)│
│         ${to_serve_op_trans_path}/{date}                        │
│  Logic:                                                          │
│    - Read extracted data (CSV, delimiter \001)                  │
│    - Read parsed_all data (previous cycle)                      │
│    - Anti-join: find records in parsed_all not in extracted     │
│    - Union new records with extracted data                      │
│    - Validate response types (isValidResponseType)               │
│    - Filter blocked client IDs                                  │
│    - Apply data quality rules (coverageid, state validation)     │
│    - Join with TU payer mapping (enrich payerid)                │
│    - Calculate transactionrange = transactionkey % trange      │
│    - Write to cooked path (Parquet, partitioned)                │
│    - Write to_serve data (Parquet)                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  email-notify-success                                            │
│  (Fork) - Parallel execution of 5 paths                         │
└───────┬───────┬───────┬───────┬───────┬─────────────────────────┘
        │       │       │       │       │
        ▼       ▼       ▼       ▼       ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│oozie_360 │ │log-      │ │email-    │ │update_   │ │lr_hive_  │
│_log_     │ │notifi-   │ │notify    │ │notifi-   │ │repair_   │
│finish    │ │cation    │ │          │ │cation    │ │cooked    │
│          │ │          │ │          │ │          │ │          │
│(Spark)   │ │(Shell)   │ │(Email)   │ │(Spark)   │ │(Hive)    │
│Log       │ │Log       │ │Send      │ │Update    │ │Repair    │
│FINISHED  │ │notifi-   │ │success   │ │notifi-   │ │cooked    │
│status    │ │cation    │ │email     │ │cation    │ │table     │
│          │ │to HDFS   │ │          │ │status    │ │partitions│
└────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
     │            │            │            │            │
     └────────────┴────────────┴────────────┴────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  email-success-end                                              │
│  (Join) - Wait for all parallel paths to complete              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  audit_procs_ErrorDecisionNode                                   │
│  (Decision) - Check if any errors occurred                      │
│  If no errors → end                                             │
│  If errors → email-fail                                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                    ┌────┴────┐
                    │  error  │
                    └────┬────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  email-fail                                                     │
│  (Fork) - Parallel execution of 2 paths                        │
└───────┬─────────────────────────────────────────────────────────┘
        │
        ▼       ▼
┌──────────┐ ┌──────────┐
│oozie_360 │ │email-    │
│_log_fail │ │fail-sent │
│          │ │          │
│(Spark)   │ │(Email)   │
│Log       │ │Send      │
│FAILED    │ │failure   │
│status    │ │email     │
└────┬─────┘ └────┬─────┘
     │            │
     └────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────┐
│  email-fail-end                                                 │
│  (Join) - Wait for both paths to complete                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  fail                                                           │
│  (Kill) - Terminate workflow with error message                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  email_fail2                                                    │
│  (Email) - Send failure email (for early failures)             │
│  Called from: check_previous_wf_status, restart_previous_     │
│              failed_wf, get_notification, check_notification,   │
│              get-date, oozie_360_log_start errors              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  fail                                                           │
│  (Kill) - Terminate workflow                                    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  end                                                            │
│  (End) - Workflow completed successfully                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 DATABRICKS PIPELINE: pl_leadrepo_escan_ich_import

```
┌─────────────────────────────────────────────────────────────────┐
│                    START                                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  lookup for trange                                              │
│  (Lookup) - Read configuration from JSON file                   │
│  Source: ADLS/{container}/data/leadrepo/config/                 │
│         ich_import_config.json                                  │
│  Extract: trange value                                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  trange                                                         │
│  (SetVariable) - Set trange variable                          │
│  Variable: trange = config.trange                              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Get breadcrumb                                                 │
│  (DatabricksNotebook) - Get breadcrumb from Cosmos DB          │
│  Notebook: /Insleads-code/Common-Util/get_breadcrumb          │
│  Source: Cosmos DB (insleads.runstatus)                         │
│  Filter: notificationtype='ie_prebdf'                          │
│  Output: business date (breadcrumb)                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Set breadcrumb                                                 │
│  (SetVariable) - Set breadcrumb as pipeline variable            │
│  Variable: dt = breadcrumb from Get breadcrumb                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                    ┌─────┴─────┐
                    │           │
                    ▼           ▼
┌──────────────────────┐ ┌──────────────────────┐
│360_logger_v1_Running │ │update_notification_  │
│                      │ │inprogress           │
│(DatabricksNotebook)  │ │                      │
│Log pipeline start    │ │(DatabricksNotebook)  │
│Notebook: 360_logger_ │ │Update notification   │
│v1                    │ │status to 'running'   │
│Target: Cosmos DB     │ │Notebook: update_     │
│(operations_log_360)  │ │notification          │
│Status: RUNNING       │ │Target: Cosmos DB     │
│                      │ │(runstatus)          │
└──────┬───────────────┘ └──────┬───────────────┘
       │                       │
       └───────────┬───────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────┐
│  lr_ich_extract_trans_demo                                      │
│  (DatabricksNotebook) - Extract transaction data from XML       │
│  Notebook: /Insleads-code/LEADREPOSITORY/escan_ich_import/     │
│           lr_ich_extract_trans_demo                            │
│  Input: ADLS/{container}/staging/input/ie/{bcdate} (XML files) │
│  Output: ADLS/{container}/data/leadrepo/transform/scratch/     │
│          ie_output/lr_ich_xml_parsed/transaction/{bcdate}       │
│         (text format, pipe-delimited)                          │
│  Logic:                                                          │
│    - Read text files from input directory                       │
│    - Parse XML using xml.etree.ElementTree                      │
│    - Extract subscriber and dependent records in single UDF     │
│    - Filter: ResponseResultStd not in ['0','3','4','5','7',     │
│              '8','10','15','16','17','312','313','314']         │
│    - Filter: ResponseResultStd.lower() != "none"                │
│    - Filter: ST01 == '271' (transaction type validation)        │
│    - Filter: non-empty TraceNumber                             │
│    - Apply business rules (groupnumber, coverageid selection)   │
│    - Filter: non-empty subscriberCoverageId                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  lr_ich_range_partitioner                                       │
│  (DatabricksNotebook) - Process and partition transaction data │
│  Notebook: /Insleads-code/LEADREPOSITORY/escan_ich_import/     │
│           lr_ich_range_partitioner                             │
│  Input: ADLS/{container}/data/leadrepo/transform/scratch/       │
│         ie_output/lr_ich_xml_parsed/transaction/{bcdate}       │
│  Output: ADLS/{container}/data/leadrepo/publish/cooked/         │
│          lr_transaction/ (partitioned by transactionrange)      │
│         ADLS/{container}/staging/input/ich_import/data/        │
│         toserve/ich_import/{bcdate}                            │
│  Logic:                                                          │
│    - Read extracted data (text, pipe-delimited)                 │
│    - Read parsed_all data (previous cycle)                      │
│    - Validate response types (isValidResponseType)              │
│    - Anti-join: find records in parsed_all not in extracted    │
│    - Union new records with extracted data                      │
│    - Apply data quality rules (coverageid, state validation)     │
│    - Filter blocked client IDs                                  │
│    - Join with TU payer mapping (enrich payerid)                │
│    - Calculate transactionrange = transactionkey % trange      │
│    - Write to cooked path (Parquet, Delta Lake append mode)    │
│    - Write to_serve data (Parquet, append mode)                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  log_notification_leadrepo_xref_update                         │
│  (DatabricksNotebook) - Log notification for downstream        │
│  Notebook: /Insleads-code/Common-Util/log_notification         │
│  Target: Cosmos DB (runstatus)                                 │
│  Notification: ie_postbdf                                      │
│  Directory: triggers                                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create_trigger_file_leadrepo_xref_update                       │
│  (DatabricksNotebook) - Create trigger file for downstream      │
│  Notebook: /Insleads-code/Common-Util/create_trigger_file      │
│  Notification: ie_postbdf                                      │
│  Breadcrumb: dt variable                                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  delete_trigger_file                                            │
│  (DatabricksNotebook) - Delete trigger file after processing    │
│  Notebook: /Insleads-code/Common-Util/delete_trigger_file     │
│  File: triggerfilename from parameters                          │
│  Path: triggerfolderpath from parameters                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  update_notification_completed                                  │
│  (DatabricksNotebook) - Update notification status to completed│
│  Notebook: /Insleads-code/Common-Util/update_notification      │
│  Target: Cosmos DB (runstatus)                                 │
│  Status: processed                                              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  breadcumb pipeline return value                                │
│  (SetVariable) - Set pipeline return value                     │
│  Variable: pipelineReturnValue.bc_return = breadcrumb          │
│  (System variable for parent pipeline)                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    END                                           │
│  Pipeline completed successfully                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 KEY DIFFERENCES SUMMARY

### **1. Initialization**
- **Hadoop**: Checks for previous failed workflows, attempts restart
- **Databricks**: Reads configuration (trange) from JSON file, no failed workflow check (handled by ADF retry policies)

### **2. Notification System**
- **Hadoop**: MapR DB (runstatus table), HDFS notification files
- **Databricks**: Cosmos DB (runstatus table), ADLS trigger files

### **3. Breadcrumb Extraction**
- **Hadoop**: Shell script (get_datetime.sh) extracts date from notification
- **Databricks**: Databricks notebook queries Cosmos DB for breadcrumb

### **4. XML Extraction**
- **Hadoop**: Pig script (lr_ich_extract_transaction_demo.pig) using XPathAll
- **Databricks**: Python/Spark notebook (lr_ich_extract_trans_demo.py) using xml.etree.ElementTree
- **Filter Difference**: Databricks has enhanced ResponseResultStd filter (excludes more codes, validates ST01='271')

### **5. Data Processing**
- **Hadoop**: Spark script (lr_ich_range_partitioner.py), writes Parquet
- **Databricks**: Databricks notebook (lr_ich_range_partitioner.py), writes Delta Lake (append mode)

### **6. Post-Processing**
- **Hadoop**: Fork with 5 parallel paths (logging, notification, email, Hive repair)
- **Databricks**: Sequential steps (log notification, create trigger, delete trigger, update notification)

### **7. Error Handling**
- **Hadoop**: Decision node checks for errors, sends failure emails
- **Databricks**: ADF handles errors via retry policies and activity failure paths

### **8. Storage**
- **Hadoop**: HDFS, MapR DB
- **Databricks**: ADLS (Azure Data Lake Storage), Cosmos DB

---

## 🔄 DATA FLOW COMPARISON

### **Hadoop Data Flow:**
```
XML Input (HDFS)
  ↓
Pig Extraction (delimited \001)
  ↓
Spark Processing (Parquet, partitioned)
  ↓
Cooked Data (HDFS Parquet)
  ↓
To Serve Data (HDFS Parquet)
```

### **Databricks Data Flow:**
```
XML Input (ADLS)
  ↓
Python/Spark Extraction (pipe-delimited text)
  ↓
Databricks Processing (Delta Lake, partitioned)
  ↓
Cooked Data (ADLS Delta Lake)
  ↓
To Serve Data (ADLS Parquet)
```

---

## ✅ VALIDATION NOTES

- Both workflows follow the same logical flow
- Core business logic is preserved
- Enhanced filtering in Databricks is a data quality improvement
- Storage and orchestration differences are platform-specific adaptations

