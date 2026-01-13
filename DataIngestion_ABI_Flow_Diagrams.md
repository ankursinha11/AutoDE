# Data Ingestion ABI Flow Diagrams

## Hadoop Workflow 1: escan_data_ingestion : ingest_all

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Oozie WF Starter Check      │
│ (Shell script)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Can Start?        │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get DateTime│    │ End         │
│ (Shell)     │    └─────────────┘
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Check GHIC                  │
│ (Shell script)              │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Fork: Group 1 (Parallel)   │
│ (3 tables)                  │
└───┬──────────────────────┬───┘
    │                      │
    ▼                      ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Process     │
│ FoundCoverage│    │ PatientAccts│
│ Segments    │    │ PayerCOB    │
│ (DLAKE)     │    │ (DLAKE)     │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ Join: Group 1               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Fork: Group 2 (Parallel)    │
│ (2 tables)                  │
└───┬──────────────────────┬───┘
    │                      │
    ▼                      ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Process     │
│ HospInsurance│    │ TPRCoverage │
│ Codes       │    │ (DLAKE)     │
│ (DLAKE)     │    │             │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ Join: Group 2               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Fork: Group 3 (Parallel)    │
│ (2 tables)                  │
└───┬──────────────────────┬───┘
    │                      │
    ▼                      ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Process     │
│ PatientAcct │    │ HelperFound │
│ StateStatus │    │ Coverages   │
│ (DLAKE)     │    │ (DLAKE)     │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ Join: Group 3               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Fork: Group 4 (Parallel)    │
│ (2 tables - Sqoop)           │
└───┬──────────────────────┬───┘
    │                      │
    ▼                      ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Process     │
│ HospPayment │    │ HospFinClass│
│ Codes       │    │ Codes       │
│ (Sqoop)     │    │ (Sqoop)     │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ Join: Group 4               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Decision: GHIC?             │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Notify GHIC │    │ Email Success│
│ (Shell)     │    │              │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ End                         │
└─────────────────────────────┘
```

## Hadoop Workflow 2: escan_data_ingestion:SA-WF:foundcoverage

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Oozie WF Starter Check      │
│ (Shell script)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Can Start?        │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get DateTime│    │ End         │
│ (Shell)     │    └─────────────┘
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Check GDemo │    │ Email Fail  │
│ (Shell)     │    │             │
└──────┬──────┘    └─────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Check GHIC                  │
│ (Shell script)              │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Process FoundCoverage       │
│ (Sub-workflow: DLAKE)       │
└──────┬──────────────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Email       │    │ Email Fail  │
│ Success     │    │             │
└──────┬──────┘    └─────────────┘
       │
       ▼
┌─────────────────────────────┐
│ End                         │
└─────────────────────────────┘
```

## Hadoop Workflow 3: escan_data_ingestion:SA-WF:patientaccts

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Oozie WF Starter Check      │
│ (Shell script)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Can Start?        │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get DateTime│    │ End         │
│ (Shell)     │    └─────────────┘
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Check GDemo │    │ Email Fail  │
│ (Shell)     │    │             │
└──────┬──────┘    └─────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Check GHIC                  │
│ (Shell script)              │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Process PatientAccts        │
│ (Sub-workflow: DLAKE)       │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Decision: GDemo?            │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Notify      │    │ Decision:   │
│ Assign Merge│    │ GHIC?       │
│ (Shell)     │    └───┬────┬────┘
└──────┬──────┘        │    │
       │               │    │
       ▼               │    │
┌─────────────┐       │    │
│ Notify      │       │    │
│ Assign Merge│       │    │
│ Swift       │       │    │
│ (Shell)     │       │    │
└──────┬──────┘       │    │
       │              │    │
       ▼              │    │
┌─────────────┐      │    │
│ Notify      │      │    │
│ Medicare    │      │    │
│ Coverage    │      │    │
│ Discovery   │      │    │
│ (Shell)     │      │    │
└──────┬──────┘      │    │
       │             │    │
       └──────┬──────┘    │
              │           │
              ▼           ▼
       ┌─────────────┐    ┌─────────────┐
       │ Notify GHIC  │    │ Email       │
       │ (Shell)      │    │ Success     │
       └──────┬───────┘    └──────┬──────┘
              │                  │
              └────────┬─────────┘
                       │
                       ▼
              ┌─────────────────────────────┐
              │ End                         │
              └─────────────────────────────┘
```

## Hadoop Workflow 4: escan_data_ingestion : sdob_ingestion

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Oozie WF Starter Check      │
│ (Shell script)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Can Start?        │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get DateTime│    │ End         │
│ (Shell)     │    └─────────────┘
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Fork: Group 1 (Parallel)     │
│ (7 tables)                   │
└───┬──────────────────────┬───┘
    │                      │
    ▼                      ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Process     │
│ TUSourced   │    │ VSnapGlobal │
│ FamilyMember│    │ MRNFamily   │
│ Link        │    │ HelperAccts │
│ (Sqoop)     │    │ (DLAKE)     │
└──────┬──────┘    └──────┬──────┘
       │                  │
       │    ... (5 more)  │
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ Join: Group 1               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ End                         │
└─────────────────────────────┘
```

## Hadoop Workflow 5: escan_data_ingestion : swift_ingest_all

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Oozie WF Starter Check      │
│ (Shell script)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Can Start?        │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Get DateTime│    │ End         │
│ (Shell)     │    └─────────────┘
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Check GDemo │    │ Email Fail  │
│ (Shell)     │    │             │
└──────┬──────┘    └─────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Check GHIC                  │
│ (Shell script)              │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Fork: Group 1 (Parallel)     │
│ (2 tables)                   │
└───┬──────────────────────┬───┘
    │                      │
    ▼                      ▼
┌─────────────┐    ┌─────────────┐
│ Process     │    │ Process     │
│ PatientAccts│    │ VSnapPatient│
│ Codes       │    │ AcctsFlags  │
│ (DLAKE)     │    │ (DLAKE)     │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ Join: Group 1               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Decision: GDemo?            │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Notify      │    │ Decision:   │
│ Assign Merge│    │ GHIC?        │
│ (Shell)     │    └───┬────┬────┘
└──────┬──────┘        │    │
       │               │    │
       ▼               │    │
┌─────────────┐       │    │
│ Notify      │       │    │
│ Assign Merge│       │    │
│ Swift       │       │    │
│ (Shell)     │       │    │
└──────┬──────┘       │    │
       │              │    │
       ▼              │    │
┌─────────────┐      │    │
│ Notify      │      │    │
│ Medicare    │      │    │
│ Coverage    │      │    │
│ Discovery   │      │    │
│ (Shell)     │      │    │
└──────┬──────┘      │    │
       │             │    │
       └──────┬──────┘    │
              │           │
              ▼           ▼
       ┌─────────────┐    ┌─────────────┐
       │ Notify GHIC  │    │ Email       │
       │ (Shell)      │    │ Success     │
       └──────┬───────┘    └──────┬──────┘
              │                  │
              └────────┬─────────┘
                       │
                       ▼
              ┌─────────────────────────────┐
              │ End                         │
              └─────────────────────────────┘
```

## Hadoop Workflow 6: escan_data_ingestion : >ingest_table - ${table}

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Ingest Data                 │
│ (Shell script - reads from  │
│  input location)            │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Reconcile?        │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Reconcile   │    │ End         │
│ Staging     │    └─────────────┘
│ (Hive)      │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Publish Data                │
│ (Shell script - moves to    │
│  served location)           │
└──────┬──────────────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Log Success │    │ Email Notify │
│ (Spark)     │    │              │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
                ▼
┌─────────────────────────────┐
│ End                         │
└─────────────────────────────┘
```

## Hadoop Workflow 7: escan_data_ingestion : >ingest_table_DLAKE - ${table}

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
           ▼
┌─────────────────────────────┐
│ Restart Previous Failed WF  │
│ (Shell script)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Restart           │
│ Successful?                 │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Preingest   │    │ End         │
│ Check       │    └─────────────┘
│ (Shell)     │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Ingestion WF Check          │
│ (Spark)                     │
└──────┬──────────────────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ 360 Log     │    │ Email Fail  │
│ Start       │    │              │
│ (Spark)     │    └─────────────┘
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│ Ingest Data                 │
│ (Shell script)              │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Reconcile?        │
└───┬────────────────────┬────┘
    │ true               │ nonreadable
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Schema Check│    │ Email Success│
│ (Shell)     │    └──────┬───────┘
└──────┬──────┘           │
       │                 │
       ▼                 │
┌─────────────────────────────┐
│ Reconcile ABI Files          │
│ (Shell script)               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Reconcile Hadoop Files      │
│ (Shell script)               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Reconcile Hadoop Files      │
│ Vacuum                      │
│ (Shell script)               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Clean Up Delta Input         │
│ (Shell script)               │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────┐
│ Decision: Notify?           │
│ (if foundcoverage)          │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Notify      │    │ Fork:      │
│ LeadRepo    │    │ Success    │
│ FC Update   │    │ (Parallel)  │
│ (Spark)     │    └───┬────┬────┘
└──────┬──────┘        │    │
       │               │    │
       └──────┬────────┘    │
              │             │
              ▼             ▼
       ┌─────────────┐    ┌─────────────┐
       │ 360 Log     │    │ Email Notify│
       │ Finish      │    │             │
       │ (Spark)     │    │             │
       └──────┬──────┘    └──────┬──────┘
              │                  │
              └────────┬─────────┘
                       │
                       ▼
              ┌─────────────────────────────┐
              │ End                         │
              └─────────────────────────────┘
```

## Databricks Pipeline 1: pl_dataingestion_abi_group1

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get BC                      │
│ (Databricks notebook)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Set BC Variable             │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ Set BC Return    │    │ 360 Logger       │
│                  │    │ Running          │
│                  │    │ (Databricks)    │
└──────┬───────────┘    └──────┬───────────┘
       │                       │
       └───────────┬───────────┘
                   │
                   ▼
          ┌─────────────────────────────┐
          │ Update Notification Running │
          │ (Databricks)                │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Wait 1                     │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Extract FNF PatientAccts   │
          │ Access Coordinator         │
          │ (Databricks)               │
          └──────┬──────────────────┘
                 │
                 ├──────────────────┐
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Extract     │    │ Extract     │
          │ PatientAccts│    │ FoundCoverage│
          │ (Databricks)│    │ (Databricks)│
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Extract     │    │ Extract     │
          │ VSnapPatient│    │ PatientAccts│
          │ AcctsFlags  │    │ Codes       │
          │ (Databricks)│    │ (Databricks)│
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Reconcile   │    │ Reconcile   │
          │ PatientAccts│    │ FoundCoverage│
          │ (Databricks)│    │ (Databricks)│
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Reconcile   │    │ Reconcile   │
          │ VSnapPatient│    │ PatientAccts│
          │ AcctsFlags  │    │ Codes       │
          │ (Databricks)│    │ (Databricks)│
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌─────────────────────────────┐
                 │ Wait 2                     │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ Log Notification           │
                 │ GlobalMRNMerged            │
                 │ (Databricks)               │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ Trigger GlobalMRNMerged    │
                 │ (Databricks)               │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ Wait 3                     │
                 └──────┬──────────────────┘
                        │
                        ├──────────────────┐
                        │                  │
                        ▼                  ▼
                 ┌─────────────┐    ┌─────────────┐
                 │ Trigger     │    │ Trigger     │
                 │ MBIHelper   │    │ FC Ingest   │
                 │ (Databricks)│    │ (Databricks)│
                 └──────┬──────┘    └──────┬──────┘
                        │                  │
                        ▼                  ▼
                 ┌─────────────┐    ┌─────────────┐
                 │ Trigger     │    │ Trigger     │
                 │ GHIC        │    │ ESPREBDF    │
                 │ (Databricks)│    │ (Databricks)│
                 └──────┬──────┘    └──────┬──────┘
                        │                  │
                        └────────┬─────────┘
                                 │
                                 ▼
                        ┌─────────────────────────────┐
                        │ Update Notification        │
                        │ Processed                  │
                        │ (Databricks)               │
                        └──────┬──────────────────┘
                               │
                               ▼
                        ┌─────────────────────────────┐
                        │ End                         │
                        └─────────────────────────────┘
```

## Databricks Pipeline 2: pl_dataingestion_abi_group2

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get BC                      │
│ (Databricks notebook)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Set BC Variable             │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐    ┌──────────────────┐
│ Set BC Return    │    │ 360 Logger       │
│                  │    │ Running          │
│                  │    │ (Databricks)    │
└──────┬───────────┘    └──────┬───────────┘
       │                       │
       └───────────┬───────────┘
                   │
                   ▼
          ┌─────────────────────────────┐
          │ Update Notification Running │
          │ (Databricks)                │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Lookup ABI Group2           │
          │ (Read table config from     │
          │  JSON file)                 │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ ForEach Table               │
          │ (Batch count: 9)            │
          └──────┬──────────────────┘
                 │
                 │ For each table:
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Extract Table               │
          │ (Databricks notebook)       │
          └──────┬──────────────────┘
                 │
                 ▼
          ┌─────────────────────────────┐
          │ Reconcile Table Delta       │
          │ (Databricks notebook)       │
          └──────┬──────────────────┘
                 │
                 │ (Next table in loop)
                 │
                 ▼
          ┌─────────────────────────────┐
          │ (All tables processed)      │
          └──────┬──────────────────┘
                 │
                 ├──────────────────┐
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Log         │    │ Log         │
          │ Notification│    │ Notification│
          │ Medicare    │    │ Commercial  │
          │ Import      │    │ Hints       │
          │ (Databricks)│    │ (Databricks)│
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Trigger     │    │ Trigger     │
          │ Medicare    │    │ Commercial  │
          │ Import      │    │ Hints       │
          │ (Databricks)│    │ (Databricks)│
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 ▼                  ▼
          ┌─────────────┐    ┌─────────────┐
          │ Log         │    │ Trigger     │
          │ Notification│    │ Medicaid    │
          │ Medicaid    │    │ Helper      │
          │ Helper      │    │ (Databricks)│
          │ (Databricks)│    │             │
          └──────┬──────┘    └──────┬──────┘
                 │                  │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌─────────────────────────────┐
                 │ Update Notification        │
                 │ Processed                  │
                 │ (Databricks)               │
                 └──────┬──────────────────┘
                        │
                        ▼
                 ┌─────────────────────────────┐
                 │ End                         │
                 └─────────────────────────────┘
```

## Key Process Steps

### Hadoop Workflows

1. **Oozie WF Starter Check** - Shell script to check if workflow can start (checks lock file)
2. **Decision: Can Start?** - If check returns true, continue; else end
3. **Get DateTime** - Shell script to extract breadcrumb date
4. **Check GDemo/GHIC** - Shell scripts to check if certain conditions are met
5. **Fork/Join** - Parallel execution of multiple table ingestion sub-workflows
6. **Sub-workflows** - Individual table processing using `ingest_table` or `ingest_table_DLAKE`
7. **Notifications** - Shell scripts to create notifications for downstream workflows
8. **Email Notifications** - Success/failure email notifications

### Databricks Pipelines

1. **Get BC** - Databricks notebook to get breadcrumb date in YMDTH format
2. **Set BC Variable** - Set breadcrumb variable from get_bc output
3. **Set BC Return** - Set pipeline return value with breadcrumb
4. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360
5. **Update Notification Running** - Update Cosmos DB runstatus table to "running"
6. **Extract** - Databricks notebook to extract data from source (ABI files or SQL Server)
7. **Reconcile Table Delta** - Databricks notebook to reconcile and publish data using Delta Lake
8. **Wait Activities** - Synchronization points for parallel activities
9. **Log Notifications** - Create notifications for downstream pipelines
10. **Trigger Files** - Create trigger files for downstream workflows
11. **Update Notification Processed** - Update Cosmos DB runstatus table to "processed"

## Key Differences

1. **Orchestration**: Hadoop uses Oozie with fork/join for parallel execution; Databricks uses Wait activities and ForEach loops
2. **Table Processing**: Hadoop has separate workflows for different table groups; Databricks uses lookup-based ForEach (group2) or explicit activities (group1)
3. **Data Extraction**: Hadoop uses shell scripts with ingest_data.sh; Databricks uses Databricks notebooks
4. **Reconciliation**: Hadoop uses Hive (ingest_table) or shell scripts with Delta Lake (ingest_table_DLAKE); Databricks uses Databricks notebooks with Delta Lake
5. **Notifications**: Hadoop uses shell scripts with MapR DB; Databricks uses Databricks notebooks with Cosmos DB
6. **Logging**: Hadoop uses Spark jobs to log to MapR DB; Databricks uses Databricks notebooks to log to Cosmos DB
7. **Error Handling**: Hadoop has email notifications on failure; Databricks relies on ADF error handling and Cosmos DB logging
8. **Workflow Starter Check**: Hadoop has explicit starter check; Databricks relies on ADF scheduling
9. **Delta Lake Processing**: Hadoop uses multiple shell scripts for ABI/Hadoop file reconciliation; Databricks uses single notebook for Delta Lake reconciliation
10. **Table Configuration**: Hadoop has hardcoded table lists in workflows; Databricks group2 uses JSON lookup file for table configuration
