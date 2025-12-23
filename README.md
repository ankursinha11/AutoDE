
For `use_container_width=True`, use `width='stretch'`. For `use_container_width=False`, use `width='content'`.
2025-12-23 20:19:56.759 | INFO     | services.langgraph.workflow:query:152 - Processing query: Can you generate logic  excel for databricks pipeline pl_leaddiscovery_leadverify?
2025-12-23 20:19:56.761 | INFO     | services.langgraph.workflow:_analyze_query_node:230 - Node: Analyze Query
2025-12-23 20:19:56.761 | INFO     | services.langgraph.workflow:_analyze_query_node:252 -   Intent: QueryIntent.EXCEL_GENERATION, Systems: [<SystemType.DATABRICKS: 'databricks'>], Filter: pipeline
2025-12-23 20:19:56.762 | INFO     | services.langgraph.workflow:_retrieve_node:271 - Node: Retrieve
2025-12-23 20:19:56.762 | DEBUG    | services.retrieval.query_rewriter:rewrite_query:90 - Rewrote query: 'Can you generate logic  excel for databricks pipeline pl_leaddiscovery_leadverify?' -> 'Can you generate logic  excel for databricks pipeline pl_leaddiscovery_leadverify? databricks databricks notebook databricks pipeline'
2025-12-23 20:19:57.036 | DEBUG    | services.retrieval.reranker:rerank:133 - Reranked 5 documents -> 5 results
2025-12-23 20:19:57.036 | INFO     | services.langgraph.workflow:_retrieve_node:335 -   Retrieved 5 documents
2025-12-23 20:19:57.038 | INFO     | services.langgraph.workflow:_read_files_node:355 - Node: Read Files
2025-12-23 20:19:57.038 | INFO     | services.langgraph.workflow:_read_files_node:383 -   Reading 5 files
2025-12-23 20:19:57.064 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\LeadDiscovery\leadverify\get_leadverify_kc_candidates.py (251 lines, python)
2025-12-23 20:19:57.064 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adf\pipeline\pl_leaddiscovery_lead_propagation.json
2025-12-23 20:19:57.065 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adf\pipeline\pl_leaddiscovery_leadlookup_knowncommercial.json
2025-12-23 20:19:57.065 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adf\pipeline\pl_leaddiscovery_leadverify.json
2025-12-23 20:19:57.065 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adf\pipeline\pl_leadrepository_xref.json
2025-12-23 20:19:57.065 | INFO     | services.langgraph.workflow:_read_files_node:425 -   Read 5 files successfully (0 failed)
2025-12-23 20:19:57.066 | INFO     | services.langgraph.workflow:_analyze_code_node:447 - Node: Analyze Code
2025-12-23 20:19:57.066 | INFO     | services.langgraph.workflow:_analyze_code_node:502 -   Analyzed 0 files (0 failed)
2025-12-23 20:19:57.067 | INFO     | services.langgraph.workflow:_generate_response_node:525 - Node: Generate Response
2025-12-23 20:19:57.067 | INFO     | services.langgraph.workflow:_generate_excel_response:1105 - Generating Excel report...
2025-12-23 20:19:57.067 | INFO     | services.langgraph.workflow:_generate_databricks_documentation_excel:1221 - Generating Databricks-only documentation Excel for: pipeline
2025-12-23 20:19:57.067 | INFO     | services.langgraph.workflow:_generate_databricks_documentation_excel:1230 - Extracting Databricks logic...
2025-12-23 20:19:57.068 | INFO     | services.stag.databricks_logic_extractor:extract_logic:66 - 📊 Extracting logic for Databricks pipeline: pipeline
2025-12-23 20:19:57.088 | INFO     | services.stag.databricks_logic_extractor:_search_pipeline_documents:122 -    Found 20 Databricks/ADF documents for pipeline
2025-12-23 20:19:57.088 | INFO     | services.stag.databricks_logic_extractor:_find_adf_json:139 -    🔍 Searching for ADF JSON: pipeline
2025-12-23 20:19:57.089 | INFO     | services.stag.databricks_logic_extractor:_find_adf_json:163 -    Trying fuzzy search for pipeline...
2025-12-23 20:19:57.091 | INFO     | services.stag.databricks_logic_extractor:_find_adf_json:196 -    Trying vector DB fallback...
2025-12-23 20:19:57.091 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:208 -    ❌ CRITICAL: ADF JSON NOT FOUND for 'pipeline'
2025-12-23 20:19:57.091 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:209 -    Searched directories:
2025-12-23 20:19:57.092 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:211 -      - ./Databricks_repo/adf/pipeline
2025-12-23 20:19:57.092 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:211 -      - /Users/ankurshome/Desktop/Hadoop_Parser/CodebaseIntelligence/Databricks_repo/*/adf/pipeline
2025-12-23 20:19:57.093 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:217 -    Available JSONs in ./Databricks_repo/adf/pipeline:
2025-12-23 20:19:57.094 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - Dashboard.json
2025-12-23 20:19:57.094 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_blacklisted_accounts.json
2025-12-23 20:19:57.094 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_cdd_adhoc_copy_to_TU.json
2025-12-23 20:19:57.094 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_cdd_bdf_download.json
2025-12-23 20:19:57.094 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_cdd_bdf_download_checkfile.json
2025-12-23 20:19:57.095 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_cdd_bdf_download_master.json
2025-12-23 20:19:57.095 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_cdd_es_postbdf.json
2025-12-23 20:19:57.095 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_cdd_es_postbdf_master.json
2025-12-23 20:19:57.095 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_cdd_es_prebdf.json
2025-12-23 20:19:57.095 | ERROR    | services.stag.databricks_logic_extractor:_find_adf_json:219 -      - pl_cdd_es_prebdf_master.json
2025-12-23 20:19:58.682 | INFO     | services.stag.databricks_logic_extractor:_extract_activities_with_ai:2113 -    ✅ AI extracted 13 activities
2025-12-23 20:19:58.682 | ERROR    | services.langgraph.workflow:_generate_databricks_documentation_excel:1310 - Databricks documentation Excel generation error: 'str' object has no attribute 'get'
2025-12-23 20:19:58.683 | INFO     | services.langgraph.workflow:_generate_response_node:550 -   Generated response (confidence: 0.70)
2025-12-23 20:19:58.684 | DEBUG    | services.langgraph.memory:add_turn:124 - Added user turn: Can you generate logic  excel for databricks pipel...
2025-12-23 20:19:58.684 | DEBUG    | services.langgraph.memory:add_turn:124 - Added assistant turn: ❌ **Error generating Databricks documentation**
