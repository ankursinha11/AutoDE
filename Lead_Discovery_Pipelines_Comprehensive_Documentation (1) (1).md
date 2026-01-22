# Lead Discovery System - Comprehensive Pipeline Documentation

## System Overview

The Lead Discovery system is an Azure Data Factory (ADF) solution designed to identify, verify, and propagate patient insurance leads. The system uses **Databricks Notebooks** for data processing, **CosmosDB** for logging status, and **HDFS/Azure Data Lake (ADLS)** for storage.

---

## Pipeline Execution Sequence

The five pipelines execute in a specific order to ensure data quality and proper lead processing:

| Order | Pipeline | Key Role | Core Requirement |
|-------|----------|----------|------------------|
| 1 | GlobalMRN Assign (31) | Identity Producer | Baseline for all IDs |
| 2 | Known Commercial (32) | Candidate Selector | Requires IDs from #31 |
| 3 | LeadLookup Known Commercial (34) | Lead Generator | Requires candidates from #32 |
| 4 | LeadVerify (35) | Quality Gate | Validates against LSB |
| 5 | Lead Propagation (33) | Distributor | Final push to EDI |

---

## Common Infrastructure Components

All five pipelines share these utility notebooks:

- **360_logger_v1**: Records "Running" and "Completed" statuses in a CosmosDB-based operations log
- **Get/Set Breadcrumb**: Manages a "breadcrumb" (BC) value, which acts as a batch timestamp or unique run identifier
- **Update Notification**: Updates the runstatus table to track if a specific notification batch is "running" or "processed"
- **Sharding Logic**: Several pipelines include db_sharding activities to split large datasets into smaller files for performance

---

## Pipeline 1: pl_leaddiscovery_globalmrn_assign

**File:** `1_pl_leaddiscovery_globalmrn_assign.py`

### Primary Function
Assigns "Global Medical Record Numbers" (Global MRNs) to candidate patient accounts to consolidate patient identities across different source systems. This is the **mandatory first step** in the entire ecosystem, functioning as the **Identity Producer**.

### Purpose & Outcome
- **Purpose**: Retrieves patient accounts from the last 90 days based on the TRG update date
- **Action**: Performs the heavy lifting of assigning PERMID, GMRN IDs, and clustered identity keys
- **Logic**: Matches accounts against the Lead Service Base (LSB) to identify existing coverage and EDI partners, filtering out accounts that already have leads sent
- **Outcome**: Creates identity-enriched patient records that downstream pipelines use for lookups. Finalized leads are exported via Sqoop to destination database (hdppatientacctxlead)

### Execution Flow

#### 1. Initialization Phase
Prepares the environment and retrieves the batch context (breadcrumb).

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **get_breadcrumb_mode** | `/Insleads-code/Common-Util/get_breadcrumb_mode` | Retrieves the run context (breadcrumb) from CosmosDB and MapR-DB for the "regular" workflow type |
| **360_logger_v1 (Running)** | `/Insleads-code/Common-Util/360_logger_v1` | Logs the initial "RUNNING" status for the module leadlookup_globalmrn_assign into the operations log |
| **update_notification (In-Progress)** | `/Insleads-code/Common-Util/update_notification` | Updates the notification status to "running" for the specific batch ID |

#### 2. Identification & Filtering Phase
Core logic where patient accounts are identified and matched.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **globalmrn_assign_get_minmaxdt** | `/Insleads-code/LeadDiscovery/globalmrn_assign/globalmrn_assign_get_minmaxdt` | Calculates the date range (min/max) for processing patient accounts based on the breadcrumb. Creates admit date lookups by hospital or EDI partner |
| **globalmrn_assign_get_candidate_patientaccts** | `/Insleads-code/LeadDiscovery/globalmrn_assign/globalmrn_assign_get_candidate_patientaccts` | Identifies potential patient accounts and sets the lead_mode_flag to "1" for discovery. Filters based on criteria like self-pay, known insurance |
| **optimized_lsb_lookup** | `/Insleads-code/LeadDiscovery/common/optimized_lsb_lookup` | Performs the cross-reference lookup against the Lead Service Base (LSB) using the candidate patient accounts. Uses keys like SSN, PermID, GlobalMRN |
| **globalmrn_assign_process_leads** | `/Insleads-code/LeadDiscovery/globalmrn_assign/globalmrn_assign_process_leads` | Processes the final lead records, applying logic for hospital admit dates and status checks. Applies filters (blacklists, billing deadlines, known coverages) and business rules |

#### 3. Validation & Export Phase
Verifies if leads are new and pushes them to the transport database.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **check_leads** | `/Insleads-code/LeadDiscovery/common/check_leads` | Compares processed leads against previously sent leads to avoid duplicates. Handles demographic changes |
| **sqoop_out** | `/Insleads-code/LeadDiscovery/common/sqoop_out` | Exports the validated leads into the hdppatientacctxlead and hdppatientacctxleadbatch SQL tables via JDBC |
| **db_sharding_to_adls** | `/Insleads-code/LeadDiscovery/common/db_sharding_to_adls` | (Conditional/Inactive) If active, shards large output files into 1000-record chunks for performance. Exports to ADLS |

#### 4. Cleanup & Completion Phase

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **delete_trigger_file** | `/Insleads-code/Common-Util/delete_trigger_file` | Removes the original trigger file from the landing zone |
| **update_notification (Completed)** | `/Insleads-code/Common-Util/update_notification` | Marks the batch status as "processed" in the run status table |

### Reference Notebooks Used
- `/Insleads-code/LeadDiscovery/common/createlookup_maxminadmitdays`
- `/Insleads-code/Common-Util/common_util`
- `/Insleads-code/Common-Util/tu_data_quality`
- `/Insleads-code/LeadDiscovery/common/get_candidate_patientaccts`
- `/Insleads-code/LeadDiscovery/common/process_leads`

---

## Pipeline 2: pl_leaddiscovery_known_commercial

**File:** `2_pl_leaddiscovery_known_commercial.py`

### Primary Function
Specifically targets the discovery of leads with "Known Commercial" insurance coverage. Acts as the **Candidate Selector**.

### Purpose & Outcome
- **Purpose**: Extracts the latest patient accounts specifically to find those eligible for "Known Commercial" insurance discovery
- **Action**: Applies commercial-specific filtering criteria to the records
- **Outcome**: Selects eligible accounts, generates a trigger file to start the next specialized lookup workflow, and publishes commercial lead data

### Execution Flow

#### 1. Initialization Phase
Sets up the run context and logging.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **Get breadcrumb** | `/Insleads-code/Common-Util/get_bc` | Retrieves the "breadcrumb" (run ID/timestamp) to track this specific execution instance |
| **360_logger_v1_Running** | `/Insleads-code/Common-Util/360_logger_v1` | Logs the start of the known_commercial module in the CosmosDB operations log |
| **update_notification_inprogress** | `/Insleads-code/Common-Util/log_notification` | Updates the run status to "running" for the knowncommercial_scheduletrigger notification type |

#### 2. Import & Extraction Phase
Brings in external configuration data and extracts raw patient data.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **sqoop_input (knowncommercial_import)** | Custom concatenated parameter | Imports configuration data (hospital or payer configurations) from SQL into the data lake to guide the extraction process. Imports hospital and configuration data from SQL Server using JDBC |
| **known_commercial_extract_data** | `/Insleads-code/LeadDiscovery/known_commercial/known_commercial_extract_data` | Extracts the relevant patient account data from the source using the configurations loaded in the previous step. Extracts from various Delta tables (e.g., patientaccts, hospitalattributes) |

#### 3. Processing & Filtering Phase
Core business logic where commercial eligibility is determined.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **known_commercial_process_data** | `/Insleads-code/LeadDiscovery/known_commercial/known_commercial_process_data` | Processes the extracted data, standardizing formats and applying initial transformations. Joins patient accounts with insurance codes and flags to identify valid commercial coverage candidates |
| **known_commercial_filter_data** | `/Insleads-code/LeadDiscovery/known_commercial/known_commercial_filter_data` | Filters the processed accounts to isolate only those that meet the specific criteria for "Known Commercial" insurance discovery |

#### 4. Publication & Export Phase
Prepares the results and exports them if valid records are found.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **known_commercial_publish_data** | `/Insleads-code/LeadDiscovery/known_commercial/known_commercial_publish_data` | Publishes the filtered list of candidate accounts to the scratch/patientaccts/ directory and returns a status (1 or 0) indicating if data exists |
| **sqoop_out (Conditional)** | Custom concatenated parameter (knowncommercial_export) | Runs only if known_commercial_publish_data returns 1 (records found). Exports the identified candidates to SQL tables (hdppatientacctxops) for reporting or intermediate storage |

#### 5. Triggering Phase
Signals the next pipeline (leadlookup_knowncommercial) to start.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **log_notification for leadlookup KC** | `/Insleads-code/Common-Util/log_notification` | Logs a new notification entry for knowncommercial in the triggers directory |
| **create trigger for leadlookup KC** | `/Insleads-code/Common-Util/create_trigger_file` | Physically creates the trigger file that the LeadLookup pipeline watches for, effectively handing off the workflow |

#### 6. Completion Phase

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **update_notification_completed** | `/Insleads-code/Common-Util/update_notification` | Marks this specific pipeline run as "processed" in the run status table |

### Reference Notebooks Used
- `/Insleads-code/Common-Util/common_util`

---

## Pipeline 3: pl_leaddiscovery_leadlookup_knowncommercial

**File:** `3_pl_leaddiscovery_leadlookup_knowncommercial.py`

### Primary Function
Performs multi-attribute identity lookups to find existing leads for commercial patient accounts. Acts as the **Lead Generator**.

### Purpose & Outcome
- **Purpose**: Performs high-intensity matching to generate leads for the accounts selected in Known Commercial pipeline
- **Action**: Uses the GMRN, PERMID, SSN, and Medical Record Number (MRN) derived in GlobalMRN Assign to scan the Lead Service Base
- **Logic**: High-intensity lookup pipeline that runs parallel processes to find matches using SSN, MRN, PermID, and Clustered Account FK
- **Outcome**: Produces a unified lead record for commercial accounts and prepares them for the EDI system. Merges disparate identity matches into a unified lead record

### Execution Flow

#### 1. Initialization Phase
Establishes the logging and tracking for the run.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **Get breadcrumb** | `/Insleads-code/Common-Util/get_breadcrumb` | Retrieves the run ID/breadcrumb context from CosmosDB for the notification type knowncommercial |
| **360_logger_v1_Running** | `/Insleads-code/Common-Util/360_logger_v1` | Logs the "RUNNING" status for the leadlookup_knowncommercial module |
| **update_notification_inprogress** | `/Insleads-code/Common-Util/update_notification` | Updates the run status to "running" in the runstatus table |

#### 2. Candidate Preparation Phase
Retrieves the date range and specific accounts to be processed.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **knowncommercial_get_minmaxdt** | `/Insleads-code/LeadDiscovery/leadlookup_knowncommercial/knowncommercial_get_minmaxdt` | Calculates the min/max date range for the batch. Generates min/max admit date lookups for the known commercial dataset |
| **knowncommercial_get_candidate_patientaccts** | `/Insleads-code/LeadDiscovery/leadlookup_knowncommercial/knowncommercial_get_candidate_patientaccts` | Identifies the specific candidate patient accounts for this run (using lead_mode_flag: "0"). Retrieves candidates that match known commercial criteria |
| **knowncommercial_process_cpa_lsb_xtable** | `/Insleads-code/LeadDiscovery/leadlookup_knowncommercial/knowncommercial_process_cpa_lsb_xtable` | Prepares the cross-reference table between Candidate Patient Accounts (CPA) and the Lead Service Base (LSB) helper table |

#### 3. Parallel Identity Lookup Phase
Most critical phase - splits into parallel branches to find leads using different identity keys.

**Branch A (Runs first):**
| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **process_globalmrnifk_leadlookup** | Pipeline-specific path | Look up leads by Global MRN |
| **process_permid_leadlookup** | Pipeline-specific path | Look up leads by PermID |

**Wait1**: Synchronization point ensuring Branch A completes

**Branch B (Runs after Wait1):**
| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **process_ssn_leadlookup** | Pipeline-specific path | Look up leads by SSN |
| **process_medicalrecnum_leadlookup** | Pipeline-specific path | Look up leads by Medical Record Number |
| **process_clusteredacctfk_leadlookup** | Pipeline-specific path | Look up leads by Clustered Account FK |

**Wait1_copy1**: Final synchronization point ensuring all lookup branches are finished

#### 4. Merge & Process Phase
Consolidates results once all lookups are complete.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **merge_cpa_xleads** | `/Insleads-code/LeadDiscovery/common/merge_cpa_xleads` | Merges the results from all the disparate lookup paths (SSN, MRN, PermID, etc.) into a single unified dataset |
| **process_leads** | `/Insleads-code/LeadDiscovery/leadlookup_knowncommercial/knowncommercial_process_leads` | Processes the merged leads, applying final business logic and status updates. Filters and refines (e.g., removing blacklisted policies, checking billing deadlines) |

#### 5. Validation & Export Phase
Checks if valid leads were generated and exports them.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **check_leads (Conditional)** | `/Insleads-code/LeadDiscovery/common/check_leads` | Runs only if process_leads output success (1). Compares the new leads against previously sent leads to prevent duplicates. Final check to determine which leads are new or updated before export |
| **sqoop_out (Conditional)** | `/Insleads-code/LeadDiscovery/common/sqoop_out` | Runs only if check_leads finds valid new records (1). Exports the final leads to the SQL table hdppatientacctxlead |

#### 6. Completion Phase

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **delete_trigger_file** | `/Insleads-code/Common-Util/delete_trigger_file` | Deletes the trigger file that started this specific run |
| **update_notification_completed** | `/Insleads-code/Common-Util/update_notification` | Marks the pipeline run as "processed" in the run status table |

### Reference Notebooks Used
- `/Insleads-code/LeadDiscovery/common/createlookup_maxminadmitdays`
- `/Insleads-code/Common-Util/common_util`
- `/Insleads-code/Common-Util/tu_data_quality`
- `/Insleads-code/LeadDiscovery/common/get_candidate_patientaccts`
- `/Insleads-code/LeadDiscovery/common/cpa_lsb_xtable`
- `/Insleads-code/LeadDiscovery/common/LeadLookupByID_1`
- `/Insleads-code/LeadDiscovery/common/process_leads`

---

## Pipeline 4: pl_leaddiscovery_leadverify

**File:** `4_pl_leaddiscovery_leadverify.py`

### Primary Function
Validates and verifies candidate leads to ensure they meet quality and business rules before they are finalized. Serves as the **Quality & Eligibility Gate**.

### Purpose & Outcome
- **Purpose**: Processes accounts that specifically require verification before being converted into active leads
- **Action**: Matches candidates against the Lead Service Base helper tables and the Lead Verification Repository to ensure the data is fresh and not redundant
- **Logic**: Applies "Lead Verify" checks, filters leads against the existing Lead Service Base (LSB), and checks the Lead Verification Repository
- **Outcome**: Outputs only verified leads, ensuring high data quality before the final push. Produces a set of verified leads and exports them for downstream consumption

### Execution Flow

#### 1. Initialization Phase
Establishes the logging and tracking for the run.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **get bc (Get Breadcrumb)** | `/Insleads-code/Common-Util/get_breadcrumb` | Retrieves the current "breadcrumb" (run identifier) for the notification type pa_lead_verification |
| **Set breadcrumb** | Internal pipeline variable | Stores the retrieved breadcrumb value into a pipeline variable (bc) for use in subsequent activities |
| **360_logger_v1_Running** | `/Insleads-code/Common-Util/360_logger_v1` | Logs the "RUNNING" status for the leadverify module into the centralized operations log |
| **update_notification_inprogress** | `/Insleads-code/Common-Util/update_notification` | Updates the run status to "running" in the runstatus table for the ID pa_lead_verification_<bc> |

#### 2. Candidate Identification Phase
Determines the scope of data to be verified.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **leadverify_get_minmaxdt** | `/Insleads-code/LeadDiscovery/leadverify/leadverify_createlookup_maxminadmitdays` | Calculates the min/max admit date range for the verification batch. Outputs results to minmaxbyhospital and minmaxbyedipartnerfk. Creates admit date lookups for the verification process |
| **leadverify_get_candidates** | `/Insleads-code/LeadDiscovery/leadverify/get_leadverify_candidates` | Identifies the candidate patient accounts that require verification. Uses inputs from permidpatientacctid and the calculated admit date lookups. Selects candidate accounts for verification |

#### 3. Verification & Lookup Phase
Core logic where leads are checked against the Lead Service Base (LSB) and verified.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **leadverify_apply_leadverify_checks** | `/Insleads-code/LeadDiscovery/leadverify/apply_leadverify_checks` | Applies specific verification business rules to the candidates, utilizing the "cooked" Lead Service Base helper data (lsbhelper). Applies verification logic (likely checking against known verified leads) |
| **leadverify_lookupleads** | `/Insleads-code/LeadDiscovery/leadverify/Leadverify_LeadLookupByID` | Performs a lookup of the candidates against the Lead Service Base using all identity keys. This step is crucial for finding any existing matches in the service base. Looks up candidates in the system to see if they already exist or match known criteria |
| **leadverify_filter_lsb_leads** | `/Insleads-code/LeadDiscovery/leadverify/filter_lsb_leads` | Filters the results from the lookup step. Separates leads that were found in the LSB (lsbfilteredleads) from those that are new or require further processing. Filters results based on Lead Service Base data |

#### 4. Processing & Repository Check Phase
Ensures that leads are not duplicates of what is already in the verification repository.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **filter_leadverifyrepo_leads** | `/Insleads-code/LeadDiscovery/leadverify/filter_leadverifyrepo_leads` | Checks the filtered leads against the leadverificationrepo to ensure they haven't been processed or verified recently. This prevents redundant work. Filters against a specific "Lead Verify Repository" to exclude or include specific records |
| **leadverify_process_leads** | `/Insleads-code/LeadDiscovery/leadverify/process_leadverify_leads` | The final processing step that formats the verified leads for export. Outputs the result to the processleads directory and returns a status (1 if leads exist, 0 otherwise). Final processing step to format valid verified leads |

#### 5. Final Validation & Export Phase
Conditionally runs only if valid leads were produced in the previous step.

| Activity | Condition | Notebook Path | Function |
|----------|-----------|--------------|----------|
| **processlead_exists (If Condition)** | Checks if leadverify_process_leads output equals 1 | N/A | Decision point |
| **leadverify_check_leads** | If processlead_exists = True | `/Insleads-code/LeadDiscovery/common/check_leads` | A final safety check to ensure these leads haven't been sent previously (checking against patientacctxlead). Returns a status (1 if valid new leads remain). Checks if the processed lead already exists to prevent duplicates |
| **checkleads_exists (If Condition)** | Checks if leadverify_check_leads output equals 1 | N/A | Decision point |
| **leadverify_sqoop_out** | If checkleads_exists = True | `/Insleads-code/LeadDiscovery/common/sqoop_out` | Exports the fully verified and checked leads to the SQL table hdppatientacctxlead using Sqoop |

#### 6. Completion Phase

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **delete_trigger_file** | `/Insleads-code/Common-Util/delete_trigger_file` | Deletes the trigger file for this run, signaling the process is complete |
| **update_notification_processed** | `/Insleads-code/Common-Util/update_notification` | Updates the final status to "processed" in the run status table |

### Reference Notebooks Used
- `/Insleads-code/Common-Util/common_util`
- `/Insleads-code/LeadDiscovery/common/createlookup_maxminadmitdays`
- `/Insleads-code/LeadDiscovery/common/get_candidate_patientaccts`
- `/Insleads-code/Common-Util/tu_data_quality`
- `/Insleads-code/LeadDiscovery/common/process_leads`

---

## Pipeline 5: pl_leaddiscovery_lead_propagation

**File:** `5_pl_leaddiscovery_lead_propagation.py`

### Primary Function
Manages the movement (propagation) of leads across different cross-reference (xref) systems. Final stage in the lifecycle as the **Publisher/Distributor**.

### Purpose & Outcome
- **Purpose**: This workflow runs multiple times daily across various lead sources (e.g., ie_xref, es_xref)
- **Action**: Uses a wider 1-year lookback based on admit date to match against "delta leads"
- **Logic**: Handles multiple notification types (e.g., ie_xref, es_xref, chc_xref). Uses logic to determine if a lead is a Community Health Center (CHC) type or a standard type and processes them accordingly. Applies final filters for existing coverage and ensures the leads haven't been sent previously
- **Outcome**: Pushes the finalized leads into the transport database (hdppatientacctxlead) for EDI transmission and cleans up trigger files. Deletes original trigger files once propagation is successful

### Execution Flow

#### 1. Initialization Phase
Determines which notification (e.g., chc_xref or ie_xref) to process and logs the start of the run.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **Get breadcrumb** | `/Insleads-code/Common-Util/get_breadcrumb_multiple_notifications_notificationtype` | Fetches the next available batch info (dt and notification_type) from the queue. Handles multiple types like ie_xref_lsb_propagation, es_propagation_famc, etc. |
| **360_logger_v1_Running** | `/Insleads-code/Common-Util/360_logger_v1` | Logs the "RUNNING" status for the leadpropagation module into the centralized operations log |
| **update_notification_inprogress** | `/Insleads-code/Common-Util/update_notification` | Updates the specific batch ID in the runstatus table to "running" to prevent other jobs from picking it up |

#### 2. Data Preparation Phase
Calculates the date windows and identifies which patient accounts are relevant for the specific notification type.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **createlookup_maxminadmitdays** | `/Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays` | Calculates the min and max admit dates (typically a 1-year lookback) to define the scope of data processing. Standard admit date lookup creation for the propagation scope |
| **get_pa (Get Patient Accounts)** | `/Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_get_pa` | Filters and retrieves the specific patient accounts associated with the current notification_type. Retrieves patient accounts eligible for lead propagation |

#### 3. Lead Processing Phase (Conditional)
The pipeline logic splits here based on notification type.

**Path A: If CHC Lead (e.g., chc_xref_lsb_propagation)**
| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **leadpropagation_process_leads_chc** | `/Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_process_leads_chc` | Processes leads using logic specific to CHC data requirements. Specialized processing for "CHC" (likely Community Health Center or a specific client type) leads |

**Path B: If Non-CHC Lead (Standard)**
| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **leadpropagation_process_leads** | `/Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_process_leads` | Processes standard leads against the "delta leads" to find coverage and apply filters. General processing of propagated leads, applying standard filters (blacklists, OHI checks) |

#### 4. Validation Check Phase (Non-CHC Only)
For standard leads, an additional check is performed to ensure duplicates are not sent.

| Activity | Condition | Notebook Path | Function |
|----------|-----------|--------------|----------|
| **check_leads** | Runs only if leadproptype is 'nonchc' and leads were successfully processed | `/Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_check_leads` | Matches the processed candidates against the patientacctxlead table to ensure they haven't been sent previously. Verifies new vs. existing leads before final output |

#### 5. Push & Distribution Phase
Exports the valid leads to the SQL transport database.

| Activity | Condition | Notebook Path | Function |
|----------|-----------|--------------|----------|
| **push_leads (sqoop_delta)** | Runs if the previous steps (CHC process OR Non-CHC check) returned valid data (1) | `/Insleads-code/LeadDiscovery/leadpropagation/sqoop_delta` | Uses Sqoop to export the final verified leads into the transport database for the EDI system. Exports the propagated leads to the target SQL database |

**Note:** There is a run_sharding activity group involving db_sharding_to_adls, but it is marked as Inactive in this pipeline.

#### 6. Completion Phase
Cleans up triggers and marks the batch as complete.

| Activity | Notebook Path | Function |
|----------|--------------|----------|
| **delete_trigger_file** | `/Insleads-code/Common-Util/delete_trigger_file` | Deletes the specific trigger file that initiated this run, clearing it from the queue |
| **update_notification_completed** | `/Insleads-code/Common-Util/update_notification` | Updates the runstatus table to "processed" for this batch ID |

### Reference Notebooks Used
- `/Insleads-code/Common-Util/common_util`
- `/Insleads-code/LeadDiscovery/common/createlookup_maxminadmitdays`
- `/Insleads-code/Common-Util/tu_data_quality`
- `/Insleads-code/LeadDiscovery/common/process_leads`

---

## Pipeline Flow Summary

### Phase One: Identity Foundation
**Step 1: GlobalMRN Assign (31)** establishes the baseline identity for all patient accounts by assigning PERMID, GMRN IDs, and clustered identity keys.

### Phase Two: Selection and Generation
**Step 2: Known Commercial (32)** selects eligible commercial insurance candidates from the identity-enriched records.

**Step 3: LeadLookup Known Commercial (34)** performs intensive parallel lookups using multiple identity keys to generate unified lead records.

### Phase Three: Quality Control and Distribution
**Step 4: LeadVerify (35)** validates leads against the Lead Service Base and Lead Verification Repository to ensure quality.

**Step 5: Lead Propagation (33)** distributes finalized leads to the transport database for EDI transmission and handles trigger file cleanup.

---

## Key Technical Patterns

### Data Storage Locations
- **HDFS Paths**: `/data/staging/`, `/data/scratch/`, `/data/processleads/`
- **SQL Tables**: `hdppatientacctxlead`, `hdppatientacctxleadbatch`, `hdppatientacctxops`
- **CosmosDB**: Operations log and breadcrumb tracking
- **MapR-DB**: Alternate breadcrumb storage

### Trigger Mechanism
Pipelines communicate through trigger files created in `/triggers/` directory. Each downstream pipeline watches for specific trigger file patterns to begin processing.

### Batch Processing
All pipelines use a "breadcrumb" (BC) value as a batch identifier, enabling:
- Parallel execution tracking
- Restart/recovery capability
- Audit trail maintenance

### Conditional Execution
Most pipelines include conditional logic based on:
- Record count (1 = records exist, 0 = no records)
- Notification type (CHC vs. Non-CHC)
- Processing success/failure status

