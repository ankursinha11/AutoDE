2025-12-24 15:40:08.554 | DEBUG    | services.stag.databricks_logic_extractor:_link_dataframes_to_tables:1187 -           Using StructType schema 'schema' for CosmosDB write
2025-12-24 15:40:08.554 | DEBUG    | services.stag.databricks_logic_extractor:_link_dataframes_to_tables:1198 -           Linked DataFrame 'df' -> CosmosDB (5 columns)
2025-12-24 15:40:08.555 | INFO     | services.stag.databricks_logic_extractor:_extract_schemas_with_write_linkage:779 -       ✅ Extracted 1 table schemas from log_notification.py
2025-12-24 15:40:08.555 | INFO     | services.stag.databricks_logic_extractor:_extract_notebook_logic_with_ai:1774 -    📋 Extracted 1 table schemas with 5 total columns
2025-12-24 15:40:08.555 | DEBUG    | services.stag.databricks_logic_extractor:_extract_notebook_logic_with_ai:1791 -    🤖 Sending to AI for semantic analysis...
2025-12-24 15:40:19.207 | INFO     | services.stag.databricks_logic_extractor:_extract_notebook_logic_with_ai:1801 -    ✅ AI extracted 30 steps, 11 snippets for /Insleads-code/Common-Util/log_notification
2025-12-24 15:40:19.207 | DEBUG    | services.stag.databricks_logic_extractor:_extract_notebook_logic_with_ai:1804 -    🔍 Extracting AI-based column lineage...
2025-12-24 15:40:28.180 | INFO     | services.stag.databricks_logic_extractor:extract_column_lineage_with_ai:2279 -    ✅ AI extracted 14 column mappings from /Insleads-code/Common-Util/log_notification
2025-12-24 15:40:28.194 | INFO     | services.stag.databricks_logic_extractor:_search_notebook_documents:1719 -    Found 20 documents for notebook: Unknown
2025-12-24 15:40:28.194 | WARNING  | services.stag.databricks_logic_extractor:_construct_file_path_from_notebook_path:1933 -    ⚠ Invalid notebook path: 'Unknown'
2025-12-24 15:40:28.195 | WARNING  | services.stag.databricks_logic_extractor:_extract_notebook_logic_with_ai:1748 -    ⚠ Cannot find file for Unknown
2025-12-24 15:40:28.195 | WARNING  | services.stag.databricks_logic_extractor:_extract_notebook_logic_with_ai:1749 -       Attempted path: None
2025-12-24 15:40:28.195 | WARNING  | services.stag.databricks_logic_extractor:_extract_notebook_logic_with_ai:1750 -       Falling back to vector DB content (column extraction will be skipped)
2025-12-24 15:40:49.207 | INFO     | services.stag.databricks_logic_extractor:extract_logic:101 - ✅ Extracted 16 activities from Databricks pipeline: pl_leaddiscovery_known_commercial
2025-12-24 15:40:49.208 | INFO     | services.langgraph.workflow:_generate_databricks_documentation_excel:1253 - Extracted 16 activities from Databricks pipeline
2025-12-24 15:40:49.208 | INFO     | services.langgraph.workflow:_generate_databricks_documentation_excel:1256 - Generating business-level documentation...
2025-12-24 15:40:49.208 | INFO     | services.stag.business_stage_abstractor:abstract_to_business_stages:60 - 🤖 Abstracting technical logic to business stages using AI
2025-12-24 15:40:49.209 | INFO     | services.stag.business_stage_abstractor:_generate_business_stages_with_rag:202 -    🤖 Sending RAG prompt to AI analyzer...
2025-12-24 15:41:01.638 | INFO     | services.stag.business_stage_abstractor:_parse_ai_response_to_stages:370 -    ✅ Parsed 6 stages from AI response
2025-12-24 15:41:01.638 | INFO     | services.stag.business_stage_abstractor:_validate_and_enrich_stages:427 -    ✅ Validated 6/6 stages
2025-12-24 15:41:01.639 | INFO     | services.stag.business_stage_abstractor:abstract_to_business_stages:73 - ✅ Generated 6 business stages
2025-12-24 15:41:01.639 | INFO     | services.langgraph.workflow:_generate_databricks_documentation_excel:1270 - Creating Excel documentation...
2025-12-24 15:41:01.640 | INFO     | services.stag.excel_generator:generate_databricks_documentation_excel:158 - 📊 Generating Databricks documentation: pl_leaddiscovery_known_commercial
2025-12-24 15:41:01.649 | INFO     | services.stag.excel_generator:generate_databricks_documentation_excel:165 -    Creating Overview sheet...
2025-12-24 15:41:01.651 | INFO     | services.stag.excel_generator:generate_databricks_documentation_excel:170 -    Creating Logic sheet...
2025-12-24 15:41:01.753 | INFO     | services.stag.excel_generator:generate_databricks_documentation_excel:174 -    Creating STTM sheet...
2025-12-24 15:41:01.754 | INFO     | services.stag.excel_generator:_create_databricks_sttm_sheet:294 -       Using AI column lineage for 360_logger_v1_Running: 15 mappings
2025-12-24 15:41:01.755 | INFO     | services.stag.excel_generator:_create_databricks_sttm_sheet:294 -       Using AI column lineage for update_notification_inprogress: 14 mappings
2025-12-24 15:41:01.758 | INFO     | services.stag.excel_generator:_create_databricks_sttm_sheet:294 -       Using AI column lineage for Get breadcrumb: 1 mappings
2025-12-24 15:41:01.759 | INFO     | services.stag.excel_generator:_create_databricks_sttm_sheet:294 -       Using AI column lineage for update_notification_completed: 10 mappings
2025-12-24 15:41:01.764 | INFO     | services.stag.excel_generator:_create_databricks_sttm_sheet:294 -       Using AI column lineage for sqoop_input: 5 mappings
2025-12-24 15:41:01.768 | INFO     | services.stag.excel_generator:_create_databricks_sttm_sheet:294 -       Using AI column lineage for sqoop_out: 4 mappings
2025-12-24 15:41:01.768 | ERROR    | services.langgraph.workflow:_generate_databricks_documentation_excel:1310 - Databricks documentation Excel generation error: 'str' object has no attribute 'get'
2025-12-24 15:41:01.769 | INFO     | services.langgraph.workflow:_generate_response_node:550 -   Generated response (confidence: 0.70)
2025-12-24 15:41:01.769 | DEBUG    | services.langgraph.memory:add_turn:124 - Added user turn: Can you generate logic excel for databricks "pl_le...
2025-12-24 15:41:01.770 | DEBUG    | services.langgraph.memory:add_turn:124 - Added assistant turn: ❌ **Error generating Databricks documentation**

'...
2025-12-24 15:41:01.795 Please replace `use_container_width` with `width`.

`use_container_width` will be removed after 2025-12-31.
