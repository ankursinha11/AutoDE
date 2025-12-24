# LeadServiceBase Flow Diagrams

## Hadoop Workflow: leadservicebase : lead gen and update

```
┌─────────────────────────────────────────────────────────────────┐
│                    START: Workflow Initialization                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  check_previous_wf_status                                        │
│  (Check for failed workflows, restart if needed)                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  get_kafka_notification                                          │
│  (Get notification from MapR DB, extract: bc, ds, base_ds)      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    decision_action_lsb                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ds = globalmrnmerge_xref_lsb?                            │  │
│  │   → lsb_association_update_decision                       │  │
│  │                                                           │  │
│  │ ds = ie_xref_lsb / fc_xref_lsb / es_xref_lsb?            │  │
│  │   → lsb_runstatus_merge_bc                               │  │
│  │                                                           │  │
│  │ Default?                                                  │  │
│  │   → process_prelsb_data                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ GMRN Merge    │  │ RunStatus Merge  │  │ Pre-LSB Data     │
│ Path          │  │ Path             │  │ Path             │
└───────┬───────┘  └────────┬─────────┘  └────────┬─────────┘
        │                    │                    │
        │                    ▼                    │
        │         ┌──────────────────────┐        │
        │         │ process_prelsb_data   │        │
        │         │ (Create demographics)│        │
        │         └──────────┬───────────┘        │
        │                    │                    │
        │                    ▼                    │
        │         ┌──────────────────────┐        │
        │         │ process_gmrn_insert   │        │
        │         │ (GMRN processing)     │        │
        │         └──────────┬───────────┘        │
        │                    │                    │
        │                    ▼                    │
        │         ┌──────────────────────┐        │
        │         │ process_permid_insert │        │
        │         │ (PermId processing)   │        │
        │         └──────────┬───────────┘        │
        │                    │                    │
        │                    ▼                    │
        │         ┌──────────────────────┐        │
        │         │   decssion_lsb       │        │
        │         │   ┌──────────────┐   │        │
        │         │   │ base_ds='fc'?│   │        │
        │         │   │   → SSN/MRN/ │   │        │
        │         │   │     Cluster  │   │        │
        │         │   │              │   │        │
        │         │   │ ds='ie_xref'?│   │        │
        │         │   │   → notify   │   │        │
        │         │   │              │   │        │
        │         │   │ Default?     │   │        │
        │         │   │   → Success  │   │        │
        │         │   └──────────────┘   │        │
        │         └──────────┬───────────┘        │
        │                    │                    │
        │                    ▼                    │
        │         ┌──────────────────────┐        │
        │         │ process_ssn_insert    │        │
        │         │ (FC only)             │        │
        │         └──────────┬───────────┘        │
        │                    │                    │
        │                    ▼                    │
        │         ┌──────────────────────┐        │
        │         │ process_mrn_insert    │        │
        │         │ (FC only)             │        │
        │         └──────────┬───────────┘        │
        │                    │                    │
        │                    ▼                    │
        │         ┌──────────────────────┐        │
        │         │ process_clusterid_    │        │
        │         │ insert (FC only)      │        │
        │         └──────────┬───────────┘        │
        │                    │                    │
        │                    └──────────┐          │
        │                               │          │
        │                    ┌───────────▼──────────┘
        │                    │
        │                    ▼
        │         ┌──────────────────────┐
        │         │ email_notify_success │
        │         │ (Fork: 5 parallel)   │
        │         └──────────┬───────────┘
        │                    │
        │                    ▼
        │         ┌──────────────────────┐
        │         │   END                │
        │         └──────────────────────┘
        │
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  lsb_association_update_decision                                │
│  (Check if FC or ICH needs updating)                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ run_fc=true?  │  │ run_ich=true?    │  │ Default?         │
│   → lsb_      │  │   → lsb_         │  │   → Success      │
│   association │  │   association    │  │                  │
│   _update_fc  │  │   _update_ich    │  │                  │
└───────┬───────┘  └────────┬─────────┘  └────────┬─────────┘
        │                    │                    │
        │                    └──────────┐          │
        │                               │          │
        │                    ┌──────────▼──────────┘
        │                    │
        │                    ▼
        │         ┌──────────────────────┐
        │         │ email_notify_success │
        │         └──────────┬───────────┘
        │                    │
        │                    ▼
        │         ┌──────────────────────┐
        │         │   END                │
        │         └──────────────────────┘
```

---

## Databricks Pipeline: pl_leadservicebase (Main Pipeline)

```
┌─────────────────────────────────────────────────────────────────┐
│                    START: Pipeline Initialization               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Set Basepath                                                   │
│  (Set basepath variable from parameters)                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  get bc                                                          │
│  (Get breadcrumb from Cosmos DB runstatus table)                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Set breadcrumb & notificationtype                              │
│  (Set dt and notification_type variables)                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  360_logger_v1_Running                                          │
│  (Log workflow start to Cosmos DB)                               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  update_notification_inprogress                                  │
│  (Update runstatus to 'inprogress')                             │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    decision_action (Switch)                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ notification_type = es_xref_lsb?                         │  │
│  │   → ES Processing Path                                    │  │
│  │                                                           │  │
│  │ notification_type = fc_xref_lsb?                         │  │
│  │   → FC Processing Path                                    │  │
│  │                                                           │  │
│  │ notification_type = ie_xref_lsb?                         │  │
│  │   → IE Processing Path                                    │  │
│  │                                                           │  │
│  │ notification_type = globalmrnmerge_xref_lsb?            │  │
│  │   → Execute pl_leadservicebase_gmrnmerge                 │  │
│  │                                                           │  │
│  │ notification_type = hfc_xref_lsb?                        │  │
│  │   → Execute pl_leadservicebase_hfc                       │  │
│  │                                                           │  │
│  │ Other notification types?                                 │  │
│  │   → Execute respective pipelines                         │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ ES Path       │  │ FC Path          │  │ IE Path          │
│               │  │                  │  │                  │
│ 1. lsb_       │  │ 1. lsb_          │  │ 1. lsb_          │
│    runstatus  │  │    runstatus     │  │    runstatus     │
│    _merge_bc  │  │    _merge_bc     │  │    _merge_bc     │
│               │  │                  │  │                  │
│ 2. process_   │  │ 2. process_      │  │ 2. process_      │
│    prelsb_    │  │    prelsb_       │  │    prelsb_       │
│    data_es    │  │    data_fc       │  │    data_ie       │
│               │  │                  │  │                  │
│ 3. process_   │  │ 3. process_      │  │ 3. process_      │
│    gmrn_      │  │    gmrn_         │  │    gmrn_         │
│    insert_es  │  │    insert_fc     │  │    insert_ie     │
│               │  │                  │  │                  │
│ 4. process_   │  │ 4. process_      │  │ 4. process_      │
│    permid_    │  │    permid_       │  │    permid_       │
│    insert_es  │  │    insert_fc     │  │    insert_ie     │
│               │  │                  │  │                  │
│               │  │ 5. process_     │  │                  │
│               │  │    ssn_insert_  │  │                  │
│               │  │    fc            │  │                  │
│               │  │                  │  │                  │
│               │  │ 6. process_     │  │                  │
│               │  │    mrn_insert_  │  │                  │
│               │  │    fc            │  │                  │
│               │  │                  │  │                  │
│               │  │ 7. process_     │  │                  │
│               │  │    clusterid_   │  │                  │
│               │  │    insert_fc    │  │                  │
│               │  │                  │  │                  │
│ 5. log_       │  │ 8. log_         │  │ 5. log_         │
│    notification│  │    notification │  │    notification │
│               │  │                  │  │                  │
│ 6. create_    │  │ 9. create_      │  │ 6. create_      │
│    trigger    │  │    trigger      │  │    trigger      │
└───────┬───────┘  └────────┬─────────┘  └────────┬─────────┘
        │                    │                    │
        │                    └──────────┐          │
        │                               │          │
        │                    ┌──────────▼──────────┘
        │                    │
        │                    ▼
        │         ┌──────────────────────┐
        │         │ update_notification_ │
        │         │ processed             │
        │         └──────────┬───────────┘
        │                    │
        │                    ▼
        │         ┌──────────────────────┐
        │         │ delete_trigger_file   │
        │         └──────────┬───────────┘
        │                    │
        │                    ▼
        │         ┌──────────────────────┐
        │         │   END                │
        │         └──────────────────────┘
```

---

## Databricks Pipeline: pl_leadservicebase_gmrnmerge (GMRN Merge Pipeline)

```
┌─────────────────────────────────────────────────────────────────┐
│                    START: GMRN Merge Pipeline                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Set breadcrumb                                                 │
│  (Set dt variable from parameter bc)                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  Set notification_type                                          │
│  (Set notification_type variable)                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  UpdateLsbLeadsReference_v1                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Step 1: Read merged gmrnIds from gmrn_xref toserve        │  │
│  │ Step 2: Get fromgmrnid lsb leads from leadservicebase     │  │
│  │ Step 3: Assign fromgmrnid lsb leads to togmrnid           │  │
│  │ Step 4: Add newgmrnid leads from LSB                      │  │
│  │ Step 5: Remove duplicate demographics per _id+hcsystem    │  │
│  │ Step 6: Rollup on _id and collect hcsystemfk_source       │  │
│  │ Step 7: Transform to LSB format                           │  │
│  │ Step 8: Insert/Update into LSB                            │  │
│  │ Step 9: Prepare leads to be deleted                       │  │
│  │ Step 10: Delete lsb leads                                 │  │
│  │ Step 11: Add to LSB Helper                                │  │
│  │ Step 12: Pass leads to Lead Propagation                   │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    END: GMRN Merge Complete                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Differences Summary

### Hadoop:
- **Single workflow** with decision nodes
- Routes based on `ds` (dataset) and `base_ds` variables
- GMRN merge handled within main workflow (separate actions for FC/ICH)
- Processes: ES, FC, IE, GlobalMRN Merge

### Databricks:
- **Main pipeline** (`pl_leadservicebase`) with Switch activity
- Routes based on `notification_type` variable
- **Separate pipeline** for GMRN merge (`pl_leadservicebase_gmrnmerge`)
- **Separate pipeline** for HFC (`pl_leadservicebase_hfc`)
- Processes: ES, FC, IE, GlobalMRN Merge, **HFC**, MH, CHC, Family Clustering

