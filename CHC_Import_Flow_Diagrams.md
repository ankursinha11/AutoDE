# CHC Import Flow Diagrams

## Hadoop Workflow 1: escan_data_ingestion : CHC

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
│ File Watcher Process        │
│ (Download files from        │
│  intermediate to input)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Log Workflow Start          │
│ (MapR DB oozie_360)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Parse and Publish           │
│ (Read input, apply mappings│
│  transform, write publish)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notification Publish        │
│ (Update MapR DB notification│
│  publish flag = '1')        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Append to Transaction       │
│ (Validate demographics,     │
│  create subscriber/dep      │
│  records, deduplicate,      │
│  append to transaction)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Generate BCS File CSV       │
│ (Join published with        │
│  transaction, create CSV)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Generate BCS File           │
│ Fixed-Width                 │
│ (Convert CSV to fixed-width │
│  using Pig)                 │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notification BCS File CR    │
│ (Update MapR DB notification│
│  bcs_file_cr flag)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Fork: Parallel Execution   │
└───┬────────────────────┬────┘
    │                    │
    ▼                    ▼
┌─────────────┐  ┌─────────────┐
│ Log Finish  │  │ Email Notify│
└──────┬──────┘  └──────┬──────┘
       │                │
       └────────┬───────┘
                │
                ▼
        ┌───────────────┐
        │ End           │
        └───────────────┘
```

## Hadoop Workflow 2: escan_data_ingestion : CHC_ID

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Date                    │
│ (Breadcrumb)                │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Check BCS File TX            │
│ (Check MapR DB for upload   │
│  notification status)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Read BCS File TX            │
│ (Read workflow ID and       │
│  breadcrumb from log)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Rows Found?       │
└───┬────────────────────┬────┘
    │ Yes (Upload)       │ No (Download)
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Upload to   │    │ Download    │
│ BCS         │    │ from BCS    │
│ (SFTP/SCP)  │    │ (SFTP/SCP)  │
└──────┬──────┘    └──────┬──────┘
       │                  │
       ▼                  │
┌─────────────┐          │
│ Decision:   │          │
│ Upload     │          │
│ Success?   │          │
└───┬────┬────┘          │
    │Yes │No             │
    ▼    ▼               │
┌────┐ ┌────┐            │
│Not │ │End │            │
│ify │ └────┘            │
│TX  │                   │
└─┬──┘                   │
  │                      │
  ▼                      │
┌────────────────────────┘
│
▼
┌─────────────────────────────┐
│ Check BCS File RX            │
│ (Check MapR DB for download  │
│  notification status)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Read BCS File RX            │
│ (Read workflow ID and       │
│  breadcrumb from log)        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Decision: Rows Found?       │
└───┬────────────────────┬────┘
    │ Yes                │ No
    ▼                    ▼
┌─────────────┐    ┌─────────────┐
│ Notification│    │ End         │
│ BCS File RX │    └─────────────┘
│ (Update MapR│
│  DB bcs_file│
│  _rx flag)  │
└──────┬──────┘
       │
       ▼
┌───────────────┐
│ End            │
└───────────────┘
```

## Hadoop Workflow 3: escan_data_ingestion : chc

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ▼
┌───────────────┐
│ End (Stub)    │
└───────────────┘
```

**Note:** This workflow is a stub with no actual processing steps.

## Databricks Pipeline: pl_chc_import

```
┌─────────────────────────────┐
│   Start                     │
└──────────┬──────────────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ Set Breadcrumb   │  │ Log Workflow     │
│ Variable         │  │ Start            │
│                  │  │ (Cosmos DB)      │
└──────┬───────────┘  └────────┬─────────┘
       │                      │
       └──────────┬───────────┘
                  │
                  ▼
┌─────────────────────────────┐
│ Parse and Publish CDA       │
│ (Read input, apply mappings │
│  with enhanced joins,       │
│  transform, write publish)  │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notification Engine         │
│ (Update Cosmos DB           │
│  published = '1')           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Append to Transaction       │
│ (Validate demographics,     │
│  create subscriber/dep      │
│  records, deduplicate,       │
│  append to transaction)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Generate BCS File           │
│ CSV and Fixed-Width         │
│ (Join published with        │
│  transaction, create CSV,   │
│  convert to fixed-width)    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Upload to BCS               │
│ (Merge files, split chunks, │
│  zip files)                 │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Get Metadata                │
│ (List files in upload path) │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ ForEach: Process Files      │
│ (Delete existing zip if     │
│  exists)                    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Convert Text to Zip         │
│ (Zip fixed-width files)     │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Copy Zip to SCP             │
│ (Upload zipped files to     │
│  TransUnion via SFTP)       │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Notification Engine         │
│ BCS Upload                  │
│ (Update Cosmos DB           │
│  bcsupload = '1')           │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ End                         │
└─────────────────────────────┘
```

## Key Process Steps

### Hadoop Workflow 1 (CHC) Steps:

1. **Check Previous Failed Workflow Status** - Check MapR DB for failed workflows
2. **Restart Previous Failed Workflow** - Attempt to restart failed workflows
3. **Get Date** - Extract breadcrumb (date) from parameters
4. **File Watcher Process** - Download/move files from intermediate to input directory
5. **Log Workflow Start** - Log RUNNING status to MapR DB oozie_360 table
6. **Parse and Publish** - Read input data, apply EID/hospital mappings, transform fields, write to publish directory
7. **Notification Publish** - Update MapR DB notification with publish flag = '1'
8. **Append to Transaction** - Validate demographics, create subscriber/dependent records, deduplicate, append to transaction table
9. **Generate BCS File CSV** - Join published data with transaction data, create CSV file
10. **Generate BCS File Fixed-Width** - Convert CSV to fixed-width format using Pig script
11. **Notification BCS File CR** - Update MapR DB notification with bcs_file_cr flag
12. **Log Workflow Finish** - Log FINISHED status to MapR DB
13. **Email Notify** - Send success email

### Hadoop Workflow 2 (CHC_ID) Steps:

1. **Get Date** - Extract breadcrumb (date) from parameters
2. **Check BCS File TX** - Check MapR DB for upload notification status
3. **Read BCS File TX** - Read workflow ID and breadcrumb from log file
4. **Decision: Upload or Download** - If rows found, upload; if not, download
5. **Upload to BCS** - Upload fixed-width files to TransUnion via SFTP/SCP (if upload needed)
6. **Notification BCS File TX** - Update MapR DB notification with bcs_file_tx flag (after upload)
7. **Download from BCS** - Download processed files from TransUnion via SFTP/SCP (after ID assignment)
8. **Check BCS File RX** - Check MapR DB for download notification status
9. **Read BCS File RX** - Read workflow ID and breadcrumb from log file
10. **Notification BCS File RX** - Update MapR DB notification with bcs_file_rx flag (after download)

### Databricks Pipeline Steps:

1. **Set Breadcrumb Variable** - Set breadcrumb from pipeline parameters
2. **Log Workflow Start** - Log RUNNING status to Cosmos DB operations_log_360
3. **Parse and Publish CDA** - Read input data, apply EID/hospital mappings (with enhanced joins), transform fields, write to publish directory
4. **Notification Engine** - Update Cosmos DB chc_tracker with published = '1'
5. **Append to Transaction** - Validate demographics, create subscriber/dependent records, deduplicate, append to transaction table
6. **Generate BCS File CSV and Fixed-Width** - Join published data with transaction data, create CSV, convert to fixed-width using Spark
7. **Upload to BCS** - Merge files, split into 15GB chunks, zip files
8. **Get Metadata** - List files in upload path
9. **ForEach: Process Files** - Delete existing zip files if they exist
10. **Convert Text to Zip** - Zip fixed-width files
11. **Copy Zip to SCP** - Upload zipped files to TransUnion via SFTP/SCP
12. **Notification Engine BCS Upload** - Update Cosmos DB chc_tracker with bcsupload = '1'

## Key Differences

- **Hadoop**: Has 3 separate workflows (CHC, CHC_ID, chc stub)
- **Databricks**: Single pipeline combining CHC and CHC_ID functionality
- **Hadoop CHC_ID**: Handles BCS file download and ID assignment processing
- **Databricks**: Does NOT include BCS file download and ID assignment (critical gap)
- **Hadoop**: Uses Pig script for fixed-width conversion
- **Databricks**: Uses Spark for both CSV and fixed-width generation in one step
- **Hadoop**: File watcher downloads files from intermediate to input
- **Databricks**: Assumes files are already in input location
- **Hadoop**: Uses MapR DB for notifications
- **Databricks**: Uses Cosmos DB for notifications
- **Databricks**: Enhanced join conditions (includes bpe and carrier fields)

