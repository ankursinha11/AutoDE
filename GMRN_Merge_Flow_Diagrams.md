# GMRN Merge - Flow Diagrams

## Pipeline Comparison
- **Hadoop**: `escan_globalmrn : merge` workflow
- **Databricks**: `pl_gmrn_merge` + `pl_gmrn_merge_sub` pipelines

---

## 🔄 HADOOP WORKFLOW: escan_globalmrn : merge

```
┌─────────────────────────────────────────────────────────────────┐
│                    START                                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  oozie_wf_starter_check                                         │
│  (Shell) - Check workflow starter file                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  check_previous_wf_status                                       │
│  (Spark) - Check MapR DB for failed workflows                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  restart_previous_failed_wf                                     │
│  (Shell) - Restart failed workflows if any                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  check_notification                                             │
│  (Shell) - Check HDFS notification (globalmrnmerged)            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  get-datetime                                                   │
│  (Shell) - Extract business date (breadcrumb)                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  gmrn_update                                                    │
│  (Spark) - Update GMRN demographics with new keys               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  move_gmrn_deltafile_creation                                   │
│  (Shell) - Move data to staging                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  publish_update_data_globalmrn                                   │
│  (Shell) - Publish updated demographics                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  gmrn_validated                                                 │
│  (Spark) - Validate GMRN demographics data                       │
│  - Check against patient accounts                               │
│  - Validate PermId references                                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  patientacct_validated                                          │
│  (Spark) - Validate patient account data                        │
│  - Check against PermId (patientacctipk only)                   │
│  - Validate lead lookup references                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_ssn_globalmrn                                            │
│  (Spark) - SSN-based identity matching                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_pid_globalmrn                                            │
│  (Spark) - PermId-based identity matching                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_simssn_globalmrn                                         │
│  (Spark) - Similar SSN matching                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_antissn_globalmrn                                        │
│  (Spark) - Anti-SSN matching                                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_mcd_globalmrn                                            │
│  (Spark) - MCD (Medicaid) matching                              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_mcr_globalmrn                                            │
│  (Spark) - MCR (Medicare) matching                              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_soundex_globalmrn                                        │
│  (Spark) - Soundex name matching                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_policyid_globalmrn                                       │
│  (Spark) - Policy ID matching                                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create_graph_data                                              │
│  (Spark) - Create graph data structures from matches            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create_graph_connections                                       │
│  (Java JAR) - Process graph connections (globalmrn-1.0.jar)     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  group_globalmrn                                                │
│  (Spark) - Group connected Global MRN records                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_decisions                                                │
│  (Spark) - Determine final merge decisions                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  assign_key_creation                                            │
│  (Spark) - Assign new Global MRN keys                           │
│  - Generate sequential keys using max + ROW_NUMBER              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create_deltafiles                                              │
│  (Spark) - Create delta files for three tables:                 │
│  - GlobalMRN (insert/delete/history)                            │
│  - GlobalMRNxPacct (insert/delete/history)                      │
│  - GlobalMRNDemographics (insert/delete/history)                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create-sqoop-gmrndemo                                          │
│  (Spark) - Create Sqoop files for demographics                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop-out-globalmrn                                            │
│  (Sqoop) - Export to GlobalMRNStaging table                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop-out-globalxpacct                                         │
│  (Sqoop) - Export to GlobalMRNxPacctStaging table               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop-out-globalmrndemo                                        │
│  (Sqoop) - Export to GlobalMRNDemographicsStaging table         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop-out-merge                                                │
│  (Sqoop) - Export to GMRNDedupeStaging table                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  move_data                                                      │
│  (Shell) - Move intermediate data to staging                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  delete_staging                                                 │
│  (Shell) - Delete staging input data                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  update_bc_status_sql                                           │
│  (Shell) - Update breadcrumb status                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop_bc_status                                                │
│  (Sqoop) - Export status to SQL Server                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create_publish_file_globalmrn                                  │
│  (Spark) - Create publish files for GlobalMRN                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  publish_data_globalmrn                                         │
│  (Shell) - Publish GlobalMRN data                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create_publish_file_globalmrnxpacct                            │
│  (Spark) - Create publish files for GlobalMRNxPacct            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  publish_data_globalmrnxpacct                                   │
│  (Shell) - Publish GlobalMRNxPacct data                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create_publish_file_globalmrnxdemo                             │
│  (Spark) - Create publish files for GlobalMRNDemographics       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  publish_data_globalmrnxdemo                                    │
│  (Shell) - Publish GlobalMRNDemographics data                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  log-notification                                               │
│  (Shell) - Log notification to HDFS                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  notify_leadrepo_gmrn_xref_update                               │
│  (Spark) - Notify Lead Repository for xref update               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  email-notify-success                                           │
│  (Fork) - Log success & send email                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    END                                           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 DATABRICKS PIPELINES: pl_gmrn_merge_sub + pl_gmrn_merge

### **Master Pipeline: pl_gmrn_merge_sub**

```
┌─────────────────────────────────────────────────────────────────┐
│                    START                                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  GMRN Merge Notifications                                       │
│  (Notebook) - Check Cosmos DB for notifications                 │
│  - Determine workflow type (regular/aid)                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Set workflowtype                                               │
│  (SetVariable) - Store workflow type                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Get breadcrumb                                                 │
│  (Notebook) - Get breadcrumb from Cosmos DB                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Set breadcrumb                                                 │
│  (SetVariable) - Store breadcrumb as 'dt'                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  360_logger_v1_Running                                          │
│  (Notebook) - Log pipeline start                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  update_notification_inprogress                                 │
│  (Notebook) - Update notification status to 'running'           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  execute gmrn_merge pipeline                                    │
│  (ExecutePipeline) - Call pl_gmrn_merge pipeline                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Wait1                                                          │
│  (Wait) - Wait 1 second                                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  AID_CHECK                                                      │
│  (IfCondition) - Check if workflow type is 'aid'                 │
└───────────┬───────────────────────────────┬─────────────────────┘
            │                               │
      (True)│                               │(False)
            ▼                               ▼
    ┌───────────────┐              ┌──────────────────────┐
    │ AID Path      │              │ REGULAR Path         │
    │ (Trigger      │              │ (Create trigger      │
    │  files)       │              │  files for           │
    │               │              │  downstream)         │
    └───────┬───────┘              └──────────┬───────────┘
            │                                │
            └────────────┬───────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Update notification to 'processed'                             │
│  (Notebook) - Update Cosmos DB status                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  360_logger_v1_Finished                                         │
│  (Notebook) - Log pipeline completion                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    END                                           │
└─────────────────────────────────────────────────────────────────┘
```

---

### **Main Pipeline: pl_gmrn_merge**

```
┌─────────────────────────────────────────────────────────────────┐
│                    START (Called from pl_gmrn_merge_sub)        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  If Workflow is Regular                                         │
│  (IfCondition) - Check if regular workflow                      │
└───────────┬───────────────────────────────┬─────────────────────┘
            │                               │
      (True)│                               │(False)
            ▼                               ▼
    ┌───────────────┐              ┌──────────────────────┐
    │ Regular Path  │              │ AID Path             │
    │ (Full merge)  │              │ (Limited processing) │
    └───────┬───────┘              └──────────┬───────────┘
            │                                │
            └────────────┬───────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  gmrn_validated                                                 │
│  (Notebook) - Validate GMRN demographics                        │
│  - Check against patient accounts                               │
│  - Validate PermId references                                   │
│  - Returns table name                                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  patientacct_validated                                          │
│  (Notebook) - Validate patient account data                     │
│  - Check against PermId (patientacctipk AND hospitalfk)         │
│  - Validate lead lookup references                              │
│  - Returns table name                                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_ssn_globalmrn                                            │
│  (Notebook) - SSN-based identity matching                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_pid_globalmrn                                            │
│  (Notebook) - PermId-based identity matching                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_simssn_globalmrn                                         │
│  (Notebook) - Similar SSN matching                              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_antissn_globalmrn                                        │
│  (Notebook) - Anti-SSN matching                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_mcd_globalmrn                                            │
│  (Notebook) - MCD (Medicaid) matching                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_mcr_globalmrn                                            │
│  (Notebook) - MCR (Medicare) matching                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_soundex_globalmrn_part01                                 │
│  (Notebook) - Soundex matching (Part 1)                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_soundex_globalmrn_part02                                 │
│  (Notebook) - Soundex matching (Part 2)                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_soundex_globalmrn_part03                                 │
│  (Notebook) - Soundex matching (Part 3)                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_policyid_globalmrn                                       │
│  (Notebook) - Policy ID matching                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  gmrn_create_graph_data                                         │
│  (Notebook) - Create graph data structures from matches         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  gmrn_process_graph_data                                        │
│  (Notebook) - Process graph connections (Spark GraphX)          │
│  - Group connected Global MRN records                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  merge_decisions                                                │
│  (Notebook) - Determine final merge decisions                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  assign_key_creation                                            │
│  (Notebook) - Assign new Global MRN keys                        │
│  - Generate sequential keys using max + ROW_NUMBER              │
│  - Format: "pid_hospitalfk" for multi-hospital support           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  insert_file_creation                                           │
│  (Notebook) - Create delta files for three tables:              │
│  - GlobalMRN (insert/delete/history)                            │
│  - GlobalMRNxPacct (insert/delete/history)                      │
│  - GlobalMRNDemographics (insert/delete/history)               │
│  - Splits "pid_hospitalfk" format to extract hospitalfk         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  If Workflow is Regular (Check)                                 │
│  (IfCondition) - Route to regular or AID path                    │
└───────────┬───────────────────────────────┬─────────────────────┘
            │                               │
      (True)│                               │(False)
            ▼                               ▼
    ┌───────────────┐              ┌──────────────────────┐
    │ pl_gmrn_merge │              │ AID-specific         │
    │ _sub_regular  │              │ processing           │
    │ (Child        │              │                      │
    │  Pipeline)    │              │                      │
    └───────┬───────┘              └──────────┬───────────┘
            │                                │
            └────────────┬───────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop_globalmrnstaging                                         │
│  (Notebook) - Export to GlobalMRNStaging (JDBC)                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop_globalmrnxpacctstaging                                   │
│  (Notebook) - Export to GlobalMRNxPacctStaging (JDBC)           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop_globalmrndemographicsstaging                             │
│  (Notebook) - Export to GlobalMRNDemographicsStaging (JDBC)     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  sqoop_gmrndedupestaging                                        │
│  (Notebook) - Export to GMRNDedupeStaging (JDBC)               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Publish data to Delta Lake                                     │
│  (Notebooks) - Publish to publish paths:                        │
│  - GlobalMRN                                                    │
│  - GlobalMRNxPacct                                              │
│  - GlobalMRNDemographics                                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Create trigger files                                           │
│  (Notebooks) - Create ADLS trigger files for:                   │
│  - Medicare                                                     │
│  - Medicaid                                                     │
│  - Global MRN Assign                                            │
│  - Global MRN Xref                                               │
│  - Lead Verify                                                   │
│  - HFC Import                                                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    END (Return to pl_gmrn_merge_sub)            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 KEY DIFFERENCES IN FLOW

### **1. Notification System**
- **Hadoop**: MapR DB + HDFS notification files
- **Databricks**: Cosmos DB + ADLS trigger files

### **2. Workflow Type Support**
- **Hadoop**: Single workflow type
- **Databricks**: Supports Regular and AID workflow types

### **3. Graph Processing**
- **Hadoop**: Java JAR (globalmrn-1.0.jar) for graph connections
- **Databricks**: Spark GraphX (Python/Spark) for graph processing

### **4. Soundex Merge**
- **Hadoop**: Single script execution
- **Databricks**: Split into 3 parts (part01, part02, part03) for performance

### **5. Data Export**
- **Hadoop**: Sqoop export to SQL Server
- **Databricks**: JDBC export to SQL Server (with hospital purge logic)

### **6. Data Publishing**
- **Hadoop**: Shell scripts (publish_data.sh, ingest_table.py)
- **Databricks**: Delta Lake merge/append operations

### **7. HospitalFK Support**
- **Hadoop**: Joins on patientacctipk only
- **Databricks**: Joins on patientacctipk AND hospitalfk (multi-hospital support)

---

## 🔄 Child Pipeline: pl_gmrn_merge_sub_regular

```
┌─────────────────────────────────────────────────────────────────┐
│                    START (Called from pl_gmrn_merge)            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  create_sqoop_gdemo                                             │
│  (Notebook) - Create Sqoop files for demographics               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    END (Return to pl_gmrn_merge)                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📝 Notes

1. **Parallel Execution**: In Databricks, some merge operations may run in parallel depending on dependencies
2. **Error Handling**: Both workflows have error handling and email notifications
3. **Delta Lake**: Databricks uses Delta Lake for ACID transactions and better data quality
4. **Table Names**: Databricks returns table names from validation notebooks for downstream use
5. **Multi-Hospital**: Databricks includes hospitalfk in all joins for correct multi-hospital matching

