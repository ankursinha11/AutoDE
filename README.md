Can you generate logic excel for Databricks " pl_leaddiscovery_lead_propagation" pipeline

✅ Databricks Documentation Excel Generated!

Pipeline: pl_leaddiscovery_lead_propagation Type: Databricks-Only Documentation (No Comparison)

📊 Contents:

Activities: 25
Business Stages: 11
Notebooks/Scripts: 0
📁 Output:

Path: outputs/databricks_documentation\Databricks_Documentation__pl_leaddiscovery_lead_propagation_20251223_184818.xlsx
Size: 174.9 KB
Sheets: 3 (Overview, Logic, STTM)
📋 Sheets Included:

Overview: Pipeline flow and architecture
Logic: Detailed activity-by-activity breakdown
STTM: Source-to-Target column mappings
Download from: outputs/databricks_documentation

📊 Query Details

Ask about your codebase...

Can you tell in detail about: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays datarbicks notebbok?

Analysis Results
📚 Evidence Sources:

leadpropagation_createlookup_maxminadmitdays.py (Relevance: 23%)

Path: LeadDiscovery\leadpropagation\leadpropagation_createlookup_maxminadmitdays.py
System: databricks
leadpropagation_createlookup_maxminadmitdays.py (Relevance: -3%)

Path: LeadDiscovery\leadpropagation\leadpropagation_createlookup_maxminadmitdays.py
System: databricks
workflow.xml (Relevance: -231%)

Path: app-lead-discovery\coordinators\leadpropagation_coord\workflow.xml
System: hadoop
workflow.xml (Relevance: -284%)

Path: app-lead-discovery\coordinators\leadpropagation_coord\workflow.xml
System: hadoop
leadverify_createlookup_maxminadmitdays.py (Relevance: -373%)

Path: LeadDiscovery\leadverify\leadverify_createlookup_maxminadmitdays.py
System: databricks
⚠️ AI Analysis Unavailable The AI analyzer could not generate a detailed response: AI analyzer not enabled or no context available Showing basic evidence-based information instead.

Confidence

70%



Id	Processing Order	Schema	Source Dataset Name	Source Field Name	Target Table/File Name	Target Field Name	Target Field Data Type	pk?	contains_pii	Field Type	Field Depends On	Pre Processing Rules	Field Definition
1	1	default	[baseurl][hdfs_input]/hospitals/current/*/	*	[baseurl][hdfs_output][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
2	1	default	[baseurl][hdfs_input]/hospitals/current/*/	*	[baseurl][hdfs_output_edipartnerfk][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
3	1	default	[baseurl][hdfs_input]/hospitalprovidernums/current/*/	*	[baseurl][hdfs_output][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
4	1	default	[baseurl][hdfs_input]/hospitalprovidernums/current/*/	*	[baseurl][hdfs_output_edipartnerfk][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
5	1	default	[baseurl][hdfs_input]/edisubmitters/current/*/	*	[baseurl][hdfs_output][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
6	1	default	[baseurl][hdfs_input]/edisubmitters/current/*/	*	[baseurl][hdfs_output_edipartnerfk][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
7	1	default	[baseurl][hdfs_input]/edipartners/current/*/	*	[baseurl][hdfs_output][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
8	1	default	[baseurl][hdfs_input]/edipartners/current/*/	*	[baseurl][hdfs_output_edipartnerfk][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
9	1	default	[baseurl][hdfs_input]/edipartnertype/current/*/	*	[baseurl][hdfs_output][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
10	1	default	[baseurl][hdfs_input]/edipartnertype/current/*/	*	[baseurl][hdfs_output_edipartnerfk][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
11	1	default	[baseurl][hdfs_input]/edipartner270settings/current/*/	*	[baseurl][hdfs_output][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
12	1	default	[baseurl][hdfs_input]/edipartner270settings/current/*/	*	[baseurl][hdfs_output_edipartnerfk][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
13	1	default	[baseurl][hdfs_input]/edipartnersubmittersoverrides/current/*/	*	[baseurl][hdfs_output][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
14	1	default	[baseurl][hdfs_input]/edipartnersubmittersoverrides/current/*/	*	[baseurl][hdfs_output_edipartnerfk][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
15	1	default	[baseurl][hdfs_input]/edipartnerhospital270settingsoverrides/current/*/	*	[baseurl][hdfs_output][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
16	1	default	[baseurl][hdfs_input]/edipartnerhospital270settingsoverrides/current/*/	*	[baseurl][hdfs_output_edipartnerfk][bc] (Parquet, overwrite)	*	Various			Bulk Transform		Notebook: /Insleads-code/LeadDiscovery/leadpropagation/leadpropagation_createlookup_maxminadmitdays	Activity: createlookup_maxminadmitdays
17	2	Azure Cosmos DB container: [cosmosdb]	Azure Cosmos DB container: [cosmosdb].[logdb] (e.g., insleads.operations_log_360) via spark.cosmos connector	*	Azure Cosmos DB container: [cosmosdb].[logdb] (e.g., insleads.operations_log_360), write mode: append	*	Various			Bulk Transform		Notebook: /Insleads-code/Common-Util/360_logger_v1	Activity: 360_running
18	3	default	CosmosDB table: runstatus (or table specified by 'maprdbtable' widget) in database 'insleads' (or as specified by 'cosmosdb' widget)	*	CosmosDB table: runstatus (or table specified by 'maprdbtable' widget) in database 'insleads' (or as specified by 'cosmosdb' widget), updated in-place via writetocosmosdb (mode: upsert/overwrite for the specific record)	*	Various			Bulk Transform		Notebook: /Insleads-code/Common-Util/update_notification	Activity: update_notification_inprogress
19	3	CosmosDB endpoint: https://prod-ent-icd-cosmos-test	CosmosDB endpoint: https://prod-ent-icd-cosmos-test.documents.azure.com:443/ (or as specified by 'cosmosendpoint' widget)	*	CosmosDB table: runstatus (or table specified by 'maprdbtable' widget) in database 'insleads' (or as specified by 'cosmosdb' widget), updated in-place via writetocosmosdb (mode: upsert/overwrite for the specific record)	*	Various			Bulk Transform		Notebook: /Insleads-code/Common-Util/update_notification	Activity: update_notification_inprogress
20	3	default	Record ID: value provided by 'id' widget	*	CosmosDB table: runstatus (or table specified by 'maprdbtable' widget) in database 'insleads' (or as specified by 'cosmosdb' widget), updated in-place via writetocosmosdb (mode: upsert/overwrite for the specific record)	*	Various			Bulk Transform		Notebook: /Insleads-code/Common-Util/update_notification	Activity: update_notification_inprogress
21	4	abfss://prod-icd-adls@prodicddls	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/data/cdd/publish/ie/imRecs/*/* (CSV)	*	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/ramcharan/data/demog_mismatch_permid/ie_imRecs_unique/ie/ (overwrite, Parquet)	*	Various			Bulk Transform		Notebook: Unknown	Activity: Set breadcrumb
22	4	abfss://prod-icd-adls@prodicddls	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/data/cdd/publish/ie/imRecs/*/* (CSV)	*	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/ramcharan/data/demog_mismatch_permid/ie_imRecs_unique/es/ (overwrite, Parquet)	*	Various			Bulk Transform		Notebook: Unknown	Activity: Set breadcrumb
23	4	abfss://prod-icd-adls@prodicddls	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/data/cdd/publish/ie/imRecs/*/* (CSV)	*	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/ramcharan/data/demog_mismatch_permid/ie_es_union_step1/ (overwrite, Parquet)	*	Various			Bulk Transform		Notebook: Unknown	Activity: Set breadcrumb
24	4	abfss://prod-icd-adls@prodicddls	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/data/cdd/publish/ie/imRecs/*/* (CSV)	*	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/ramcharan/data/demog_mismatch_permid/lsb_ich_step2/ (overwrite, Parquet)	*	Various			Bulk Transform		Notebook: Unknown	Activity: Set breadcrumb
25	4	abfss://prod-icd-adls@prodicddls	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/data/cdd/publish/ie/imRecs/*/* (CSV)	*	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/ramcharan/data/demog_mismatch_permid/ca_dob_notmatching/ (overwrite, Parquet)	*	Various			Bulk Transform		Notebook: Unknown	Activity: Set breadcrumb
26	4	abfss://prod-icd-adls@prodicddls	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/data/cdd/publish/ie/imRecs/*/* (CSV)	*	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/ramcharan/data/demog_mismatch_permid/ca_ssn_notmatching/ (overwrite, Parquet)	*	Various			Bulk Transform		Notebook: Unknown	Activity: Set breadcrumb
27	4	abfss://prod-icd-adls@prodicddls	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/data/cdd/publish/ie/imRecs/*/* (CSV)	*	abfss://prod-icd-adls@prodicddls.dfs.core.windows.net/ramcharan/data/demog_mismatch_permid/ca_final_step3/ (overwrite, Parquet)	*	Various			Bulk Transform		Notebook: Unknown	Activity: Set breadcrumb
<img width="4201" height="1065" alt="image" src="https://github.com/user-attachments/assets/77df5884-6002-4ceb-af93-42351d659b04" />
