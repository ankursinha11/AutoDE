 STAG - Smart Transform Analysis
🤖 Powered by AI agents with visible thinking process

Can you tell me about Databricks "pl_leadservicebase_famc"

Here is a comprehensive analysis of the Databricks pipeline "pl_leadservicebase_famc" based on the full content of the provided scripts and pipeline definitions:

1. Business Purpose
"pl_leadservicebase_famc" is a Databricks pipeline orchestrated via Azure Data Factory (ADF) that processes and enriches policy and demographic data for the "lead service base" (LSB) in the FAMC domain. The pipeline extracts, transforms, and loads (ETL) data from various sources, merges and validates it, and prepares it for downstream consumption in lead management, likely for healthcare or financial services.

The pipeline is designed to:

Extract policy information and demographic data.
Merge and validate lead records.
Populate reference tables for lead lookups.
Log and trigger notifications for downstream processes.
2. Inputs (Tables, Files, Data Sources)
Inputs are passed as parameters to Databricks notebooks and typically reference ADLS Gen2 paths or Delta tables. Key input sources include:

input_ca_permid_trans: /data/leadrepo/staging/famc/famc_lr_xref_permid_transaction/

Cross-reference of permid (person identifier) and transaction data.
input_lr_transaction: /data/leadrepo/publish/cooked/lr_transaction/

Lead repository transaction data.
input_permid_x_patientaccts: /data/cdd/publish/permidpatientacctid/*/*

Mapping between permid and patient accounts.
input_foundcoverage: /data/dataingestion/publish/foundcoverage/current/20991231/

Data about found coverage for patients.
input_patientaccts: /data/dataingestion/publish/patientaccts/current/20991231/

Patient account data.
dataingestion_path: /data/dataingestion/publish/

General path for ingestion data.
CosmosDB (runstatus table): Used for status tracking and logging.

Other configuration and helper paths:

/data/leadservicebase/config/
/data/leadservicebase/publish/cooked/leadservicebasehelper/
/data/leadservicebase/publish/served/leadservicebase/
/data/leadrepo/publish/cooked/leadstatus/
/data/leaddiscovery/config/mappededidatasourcefk
All paths are parameterized and passed to notebooks as baseParameters.

3. Outputs (Target Tables, Files)
Outputs are written to ADLS Gen2 paths and Delta tables, including:

output_stage: /data/leadservicebase/staging/famc/

Staging area for processed lead service base data.
deltalake_output: /data/leadservicebase/publish/served/leadservicebase/

Final published/served lead service base data.
leadlookupoppath: /data/leadservicebase/publish/cooked/leadservicebasehelper/

Helper output for lead lookups.
Other outputs:

Temporary and scratch paths for intermediate data.
Reference tables for lead lookups (e.g., famc_PopulateLsbLeadsReference).
4. Key Transformations
The pipeline consists of several orchestrated Databricks notebooks, each performing specific transformations:

a. Policy Info Extraction (famc_get_policyinfo)
Extracts policy information using provided transaction and cross-reference data.
Merges data from multiple sources (permid, transaction, patient accounts).
Applies business context (bc), user, and feed source parameters.
b. Demographic Extraction (famc_demog_extraction_fc, famc_demog_extraction_pa, famc_demog_extraction_tu)
Extracts and transforms demographic information for leads.
Processes found coverage and patient account data.
Writes outputs to staging and publish paths.
c. Decision Update (Update_decision)
Determines whether to process data as "fc" (found coverage) or "ich" (incomplete coverage/hospital).
Sets workflow branching for subsequent steps.
d. Lead Reference Population (famc_PopulateLsbLeadsReference)
Populates reference tables for lead lookups.
Uses configuration files and helper tables.
Handles partitioning and indexing for efficient lookup.
e. Logging and Notification
Logs pipeline status and activity to CosmosDB (log_notification_leadpropagation_famc).
Creates trigger files for downstream processes.
**f. Runstatus Merge (`lsb_runstatus_merge
📊 Query Details

can you generate an excel for Databricks "pl_leadservicebase_famc" logic

Transformation Logic Analysis
Query: can you generate an excel for Databricks "pl_leadservicebase_famc" logic

Snippet 0
Business Purpose: This code processes and prepares lead data for analysis to support business development activities.

Complexity: LOW

Data Quality Rules:

No explicit data quality rules are applied in the provided code snippet.
No null value validation is present.
No deduplication logic is present.
No data type enforcement or schema validation is present.
No date format standardization is present.
No email address or pattern validation is present.
No range or domain checks are present.
No reference data lookups or foreign key validation is present.
No outlier or anomaly detection is present.
No logging or error handling for data quality issues is present.
Snippet 1
Business Purpose: This code processes and publishes lead reference data for analytics and reporting in a healthcare context.

Complexity: MEDIUM

Data Quality Rules:

Deduplication based on "transactionkey" field
Null value validation on "ssn" field
Standardization of date formats using DateTimeFormatter
Validation of partition count for data distribution ("num_partition")
Validation of lookup values in "lookupvalue" column
Referential integrity check using "sec_index_name" (secondary index)
Configuration-driven rule enforcement via external "config_file_path"
Validation of source system identifier ("source" field)
Snippet 2
Business Purpose: This code processes and manages lead data inputs and outputs for streamlined lead discovery operations.

Complexity: LOW

Data Quality Rules:

Parameter presence validation for user, container, storageaccount, bc, hdfs_input, hdfs_leads_input, hdfs_admitdt_path, hdfs_output, and hdfs_maprdb_input via widget inputs
📊 Query Details

please generate stag for databricks pl_leaddiscovery_leadlookup_knowncommercial

No response generated

📊 Query Details

Ask about your codebase...

can you generate sttm for databricks pl_leaddiscovery_leadlookup_knowncommercial

💭 Analyzing query: "can you generate sttm for databricks pl_leaddiscovery_leadlookup_knowncommercial..."

💭 Intent detected: DOCUMENT_GENERATION (confidence: 98%)

💭 Reasoning: Detected document generation request - will generate docs/STTM (Also detected: lineage)

💭 Entities detected: pl_leaddiscovery_leadlookup_knowncommercial

💭 Systems involved: databricks

💭 Multi-intent query detected. Secondary intents: lineage

📋 Task Plan:
Find workflow/pipeline: pl_leaddiscovery_leadlookup_knowncommercial
Extract complete metadata and transformations
Generate STTM mappings if needed
Create document (Excel/JSON/Markdown)
Provide download link
Parse entity: pl_leaddiscovery_leadlookup_knowncommercial
Extract column-level mappings (STTM)
Build dependency chains
Trace lineage across transformations
Generate lineage visualization data
⚙️ Task 1/5: Finding workflow/pipeline...

✅ Found 5 related files

⚙️ Task 2/5: Reading actual script files...

✅ Read file: pl_leaddiscovery_leadlookup_knowncommercial.json

⚙️ Task 3/5: Generating STTM mappings using AI...

↳ Generated 0 STTM mappings

✅ Extracted 0 STTM mappings

⚙️ Task 4/5: Generating document...

✅ Document generated: outputs\generated_documents\sttm_pl_leaddiscovery_leadlookup_knowncommercial.json_20251222_234001.xlsx

⚙️ Task 5/5: Formatting response...

💡 Answer:
📄 Document Generated for pl_leaddiscovery_leadlookup_knowncommercial.json
System: DATABRICKS File Path: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\Databricks_repo\adf\pipeline\pl_leaddiscovery_leadlookup_knowncommercial.json

📊 STTM Mappings Generated
Found 0 column-level mappings

📥 Download
Document saved to: outputs\generated_documents\sttm_pl_leaddiscovery_leadlookup_knowncommercial.json_20251222_234001.xlsx

You can download this file from the outputs/generated_documents directory.
