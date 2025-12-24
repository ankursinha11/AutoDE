# Lead Repository XREF - Flow Diagrams

## Hadoop Workflow: `leadrepository : xref_table_update`

```
┌─────────────────────────────────────────────────────────────────┐
│                    START: Workflow Initialization               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │  check_previous_wf_status              │
        │  (oozie_360_wf_checker.py)             │
        │  Check failed workflows in MapR DB     │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  restart_previous_failed_wf            │
        │  (oozie_360_wf_runner_v1.sh)           │
        │  Restart failed workflows              │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  oozie_runner_decision                 │
        │  If restart needed → continue          │
        │  Else → END                             │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  get_notification                      │
        │  (get_notification.py)                  │
        │  Get notifications from MapR DB         │
        │  Filter: notificationtype='ALL',        │
        │          category='xref'                │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  check_notification                    │
        │  (check_notification_v2.0.sh)          │
        │  Validate notification                 │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  decisionnode                          │
        │  If startjob='true' → continue         │
        │  Else → END                             │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  get-date                              │
        │  (get_datetime.sh)                     │
        │  Extract business date (breadcrumb)     │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  oozie_360_log_start                  │
        │  (oozie_360_logger_v1.py)              │
        │  Log workflow start                    │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  decisionnode-flow                     │
        │  Route based on notification source:   │
        │  • escan_globalmrn_merge → GMRN path   │
        │  • fc_xref → FC path                   │
        │  • Default (ie/es_postbdf) → ICH path  │
        └─────┬───────────┬───────────┬──────────┘
              │           │           │
    ┌─────────┘           │           └─────────┐
    │                     │                     │
    ▼                     ▼                     ▼
┌──────────┐        ┌──────────┐        ┌──────────┐
│ GMRN     │        │ FC       │        │ ICH      │
│ PATH     │        │ PATH     │        │ PATH     │
└──────────┘        └──────────┘        └──────────┘
    │                     │                     │
    │                     │                     │
    ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────┐
│  GMRN PATH: escan_globalmrn_merge                   │
├─────────────────────────────────────────────────────┤
│  1. lr_update_gmrn_xref                             │
│     (lr_update_gmrnid_xref.py)                      │
│     Update GMRN IDs in xref table                   │
│                                                     │
│  2. publish_updated_gmrnmerge                       │
│     (publish_xref.sh)                               │
│     Publish to MapR DB                              │
└─────────────────────────────────────────────────────┘
    │
    └─────────────────────────────────────────┐
                                               │
    ┌─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  FC PATH: fc_xref                                   │
├─────────────────────────────────────────────────────┤
│  1. lr_xref_fc_gmrnid_insert                        │
│     (leadrepo_gmrnxfc_cooked.py)                   │
│     Create FC GMRN xrefs                            │
│                                                     │
│  2. publish_fc_gmrnid                               │
│     (publish_xref.sh)                               │
│     Publish FC GMRN xrefs                           │
│                                                     │
│  3. lr_xref_fc_permid_insert                        │
│     (leadrepo_permidxfc_cooked.py)                  │
│     Create FC PermId xrefs                          │
│                                                     │
│  4. publish_fc_permid                               │
│     (publish_xref.sh)                               │
│     Publish FC PermId xrefs                         │
│                                                     │
│  5. lr_xref_fc_ssn_mrn_cluster_insert               │
│     (leadrepo_ssn_mrn_cluster_xfc_cooked.py)        │
│     Create FC SSN/MRN/Cluster xrefs                 │
│                                                     │
│  6. publish_fc_ssn_mrn_clusterid                     │
│     (publish_xref.sh)                               │
│     Publish FC SSN/MRN/Cluster xrefs                │
└─────────────────────────────────────────────────────┘
    │
    └─────────────────────────────────────────┐
                                               │
    ┌─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  ICH PATH: ie_postbdf / es_postbdf                  │
├─────────────────────────────────────────────────────┤
│  1. lr_xref_permid_insert                           │
│     (lr_create_xref_perm.py)                       │
│     Create ICH PermId xrefs                         │
│                                                     │
│  2. publish_ich_permid                              │
│     (publish_xref.sh)                               │
│     Publish ICH PermId xrefs                       │
│                                                     │
│  3. lr_xref_gmrnid_insert                           │
│     (lr_ich_xref_cook_gmrnid_trans.py)              │
│     Create ICH GMRN xrefs                           │
│                                                     │
│  4. publish_ich_gmrnid                              │
│     (publish_xref.sh)                               │
│     Publish ICH GMRN xrefs                          │
└─────────────────────────────────────────────────────┘
    │
    └─────────────────────────────────────────┐
                                               │
    ┌─────────────────────────────────────────┘
    │
    ▼
        ┌────────────────────────────────────────┐
        │  email-notify-success (FORK)            │
        │  Parallel execution:                    │
        │  • oozie_360_log_finish                │
        │  • log-notification                    │
        │  • lsb-notification                    │
        │  • update_notification                 │
        │  • email-notify                        │
        │  • famc_decisionnode_flow               │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  email-success-end (JOIN)               │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  audit_procs_ErrorDecisionNode          │
        │  If no errors → END                     │
        │  Else → email-fail                     │
        └────────────────────────────────────────┘
```

---

## Databricks Pipelines: `pl_leadrepository_xref` + Child Pipelines

```
┌─────────────────────────────────────────────────────────────────┐
│         START: pl_leadrepository_xref (Master Pipeline)        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │  get bc                                 │
        │  (get_breadcrumb_multiple_notifications │
        │   _notificationtype)                    │
        │  Get breadcrumb from Cosmos DB          │
        │  Filter: notificationtype=              │
        │    'fc_xref,ie_postbdf,es_postbdf,      │
        │     escan_globalmrn_merge'              │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  Set breadcrumb                        │
        │  Set pipeline variable 'bc'            │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  notificationtype                      │
        │  Set pipeline variable                 │
        │  'notification_type'                   │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  360_logger_v1_Running                 │
        │  (360_logger_v1)                       │
        │  Log pipeline start to Cosmos DB       │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  update_notification_inprogress        │
        │  (update_notification)                 │
        │  Update notification status to         │
        │  'running' in Cosmos DB                │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  Switch1                               │
        │  Route based on notification_type:     │
        │  • fc_xref → pl_fc_xref                │
        │  • ie_postbdf → pl_ich_xref            │
        │  • es_postbdf → pl_ich_xref            │
        │  • escan_globalmrn_merge →             │
        │    pl_globalmrn_xref                   │
        └─────┬───────────┬───────────┬──────────┘
              │           │           │
    ┌─────────┘           │           └─────────┐
    │                     │                     │
    ▼                     ▼                     ▼
┌──────────┐        ┌──────────┐        ┌──────────┐
│ FC       │        │ ICH       │        │ GMRN     │
│ PATH     │        │ PATH      │        │ PATH     │
└──────────┘        └──────────┘        └──────────┘
    │                     │                     │
    │                     │                     │
    ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────┐
│  FC PATH: pl_fc_xref (Child Pipeline)              │
├─────────────────────────────────────────────────────┤
│  1. lr_xref_fc_gmrnid_insert                        │
│     (leadrepo_gmrnxfc_cooked.py)                   │
│     Create FC GMRN xrefs                            │
│     Filter: inferredflag=0, hitstatus in (1,2)     │
│     Join: patientacctifk + hospitalfk              │
│     _id: gmrnid_transactionkey_hospitalfk_sourcekey│
│                                                     │
│  2. lr_xref_fc_permid_insert                        │
│     (leadrepo_permidxfc_cooked.py)                  │
│     Create FC PermId xrefs                          │
│     Filter: inferredflag=0, hitstatus in (1,2)       │
│     Join: patientacctifk + hospitalfk              │
│     _id: permid_transactionkey_hospitalfk_sourcekey│
│                                                     │
│  3. lr_xref_fc_ssn_mrn_cluster_insert               │
│     (leadrepo_ssn_mrn_cluster_xfc_cooked.py)        │
│     Create FC SSN/MRN/Cluster xrefs                 │
│     Filter: inferredflag=0, hitstatus in (1,2)      │
│     Join: patientacctifk + hospitalfk              │
│     _id: transactionkey_hospitalfk_sourcekey       │
│                                                     │
│  4. ForEach_fc_xref (Publish all FC tables)         │
│     (Generic_Delta_xref_Publish.py)                │
│     Publish to Delta Lake using ACID transactions   │
└─────────────────────────────────────────────────────┘
    │
    └─────────────────────────────────────────┐
                                               │
    ┌─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  ICH PATH: pl_ich_xref (Child Pipeline)             │
├─────────────────────────────────────────────────────┤
│  Parameters: module_name (ie_postbdf or es_postbdf) │
│                                                     │
│  1. lr_create_xref_perm                             │
│     (lr_create_xref_perm.py)                       │
│     Create ICH PermId xrefs                         │
│     Uses RANK() for deduplication                   │
│     _id: permid-tracenumber-hospitalfk-sourcekey   │
│                                                     │
│  2. lr_ich_xref_cook_gmrnid_trans                   │
│     (lr_ich_xref_cook_gmrnid_trans.py)              │
│     Create ICH GMRN xrefs                            │
│     _id: gmrnid-transactionkey-clientid-sourcekey  │
│                                                     │
│  3. ForEach_ich_xref (Publish all ICH tables)       │
│     (Generic_Delta_xref_Publish.py)                 │
│     Special deduplication for PermId:               │
│     ROW_NUMBER() OVER (PARTITION BY                 │
│       transactionkey,hospitalfk,sourcekey,permid   │
│       ORDER BY p_xrefsource ASC)                    │
│     Publish to Delta Lake using ACID transactions   │
└─────────────────────────────────────────────────────┘
    │
    └─────────────────────────────────────────┐
                                               │
    ┌─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  GMRN PATH: pl_globalmrn_xref (Child Pipeline)       │
├─────────────────────────────────────────────────────┤
│  1. lr_update_gmrn_xref                             │
│     (lr_update_gmrnid_xref.py)                      │
│     Update GMRN IDs in xref table                   │
│     Reads from Delta Lake, joins with merge         │
│     decisions CSV                                   │
│                                                     │
│  2. Publish (Delta Lake merge operation)            │
│     Updates existing records using Delta merge      │
└─────────────────────────────────────────────────────┘
    │
    └─────────────────────────────────────────┐
                                               │
    ┌─────────────────────────────────────────┘
    │
    ▼
        ┌────────────────────────────────────────┐
        │  After each child pipeline completes:  │
        │  • log_notification_leadservicebase_*   │
        │    (log_notification)                   │
        │    Log notification to Cosmos DB        │
        │                                         │
        │  • create trigger file *                │
        │    (create_trigger_file)                │
        │    Create trigger file for downstream   │
        │    pipelines                            │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  360_logger_v1_Finished                │
        │  (360_logger_v1)                       │
        │  Log pipeline completion to Cosmos DB  │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  update_notification_complete          │
        │  (update_notification)                 │
        │  Update notification status to         │
        │  'complete' in Cosmos DB               │
        └────────────┬───────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────────────┐
        │  END                                    │
        └────────────────────────────────────────┘
```

---

## Key Differences in Flow

### **1. Routing Mechanism**
- **Hadoop**: Single workflow with decision node routing to different action paths
- **Databricks**: Master pipeline with Switch activity routing to separate child pipelines

### **2. Pipeline Structure**
- **Hadoop**: All processing in one workflow, sequential actions
- **Databricks**: Modular design with separate pipelines:
  - `pl_leadrepository_xref` (Master)
  - `pl_fc_xref` (FC processing)
  - `pl_ich_xref` (ICH processing)
  - `pl_globalmrn_xref` (GMRN merge processing)

### **3. Notification System**
- **Hadoop**: MapR DB for notifications
- **Databricks**: Cosmos DB for notifications

### **4. Data Storage**
- **Hadoop**: MapR DB (Parquet files) with shell scripts for publishing
- **Databricks**: Delta Lake with ACID transactions for publishing

### **5. Deduplication**
- **Hadoop**: Basic deduplication using RANK() for ICH PermId
- **Databricks**: Enhanced deduplication with ROW_NUMBER() in publishing step for ICH PermId

### **6. Error Handling**
- **Hadoop**: Custom workflow checker and restart mechanism
- **Databricks**: ADF native retry and error handling

---

## Pipeline Connection Details (Databricks)

### **Master Pipeline (`pl_leadrepository_xref`)**
- Retrieves breadcrumb and notification type from Cosmos DB
- Routes to appropriate child pipeline based on notification type
- Manages logging and notification updates
- Waits for child pipeline completion before proceeding

### **Child Pipeline: `pl_fc_xref`**
- Called by master when `notification_type = 'fc_xref'`
- Processes FC xref data in sequence:
  1. GMRN xrefs
  2. PermId xrefs
  3. SSN/MRN/Cluster xrefs
- Publishes all FC tables to Delta Lake
- Returns control to master pipeline

### **Child Pipeline: `pl_ich_xref`**
- Called by master when `notification_type = 'ie_postbdf'` or `'es_postbdf'`
- Receives `module_name` parameter to distinguish between IE and ES
- Processes ICH xref data in sequence:
  1. PermId xrefs (with RANK() deduplication)
  2. GMRN xrefs
- Publishes with special ROW_NUMBER() deduplication for PermId
- Returns control to master pipeline

### **Child Pipeline: `pl_globalmrn_xref`**
- Called by master when `notification_type = 'escan_globalmrn_merge'`
- Updates GMRN IDs in xref table
- Uses Delta Lake merge operations
- Returns control to master pipeline

---

## Data Flow Summary

### **Hadoop:**
```
Notification (MapR DB) → Decision Node → Processing Path → Publish (MapR DB) → Notifications
```

### **Databricks:**
```
Notification (Cosmos DB) → Master Pipeline → Switch → Child Pipeline → Delta Lake → Notifications (Cosmos DB)
```

