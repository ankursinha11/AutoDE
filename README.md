use_container_width=False`, use `width='content'`.
🔍 ENHANCED PARSING - EXTRACTING ALL COMPONENTS
============================================================
   📊 Extracting graphs with fixed name parsing...
      • Found 150 graphs with corrected names

📊 FLOW TYPE ANALYSIS:
   Total flows: 855
   Flow connections: 1710
   📋 FLOW TYPE DISTRIBUTION:
      • PARAMETER_FLOW: 855
   📋 CONFIG FLOWS IDENTIFIED: 855

✅ ENHANCED PARSING COMPLETE:
   📊 COMPONENT SUMMARY:
      • Total Graphs: 150
      • Total Vertices: 1295
      • Total Flows: 855
      • Total Ports: 6116
      • Total Flow Connections: 1710
      • Total Port Bindings: 728
      • Total Config Flows: 855
      • Total Graph Vertex Links: 1089
      • Total Graph Flow Links: 855

💾 Enhanced components saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\1500_CDD_TUSourcedFamilyMemberLink_components.json
2025-11-19 20:54:33.839 | INFO     | services.local_search.local_search_client:index_documents:105 - Indexing 1 documents...
2025-11-19 20:54:33.841 | INFO     | services.local_search.local_search_client:index_documents:155 - Generating embeddings...
Batches: 100%|███████████████| 1/1 [00:00<00:00, 13.66it/s] 
2025-11-19 20:54:34.154 | INFO     | services.local_search.local_search_client:index_documents:262 - ✓ Indexed 1 documents successfully (upserted)
2025-11-19 20:55:05.002 | INFO     | services.stag.stag_orchestrator:generate_comparison:92 - ================================================================================      
2025-11-19 20:55:05.002 | INFO     | services.stag.stag_orchestrator:generate_comparison:93 - 🎯 STAG COMPARISON: ABINITIO → Databricks
2025-11-19 20:55:05.002 | INFO     | services.stag.stag_orchestrator:generate_comparison:94 - Source Workflow: 1500_CDD_TUSourcedFamilyMemberLink
2025-11-19 20:55:05.003 | INFO     | services.stag.stag_orchestrator:generate_comparison:95 - ================================================================================      
2025-11-19 20:55:05.003 | INFO     | services.stag.stag_orchestrator:generate_comparison:111 -
📋 Step 1: Looking up Databricks mapping...
2025-11-19 20:55:05.004 | INFO     | services.stag.system_mapping_service:get_databricks_mapping:112 - ✅ Fuzzy match found: '1500_CDD_TUSourcedFamilyMemberLink' → '1500_CDD_TUSourcedFamilyMemberLink.pset' (similarity: 92.75%)
2025-11-19 20:55:05.004 | INFO     | services.stag.stag_orchestrator:generate_comparison:141 -    ✅ Found mapping: 1500_CDD_TUSourcedFamilyMemberLink → ['pl_TUSourcedFamilyMemberLink']
2025-11-19 20:55:05.004 | INFO     | services.stag.stag_orchestrator:generate_comparison:150 -
📊 Step 2: Extracting abinitio logic...
2025-11-19 20:55:05.004 | INFO     | services.stag.abinitio_logic_extractor:extract_logic:93 - 📊 Extracting logic for Ab Initio graph: 1500_CDD_TUSourcedFamilyMemberLink
2025-11-19 20:55:05.059 | INFO     | services.stag.abinitio_logic_extractor:_search_graph_documents:253 -    Found 2 Ab Initio documents for 1500_CDD_TUSourcedFamilyMemberLink     
2025-11-19 20:55:05.060 | WARNING  | services.stag.abinitio_logic_extractor:_get_parsed_json_path:274 -    Parsed JSON not found: 
2025-11-19 20:55:05.886 | INFO     | services.stag.abinitio_logic_extractor:_extract_steps_with_ai:640 -    ✅ AI extracted 2 steps
2025-11-19 20:55:05.887 | INFO     | services.stag.abinitio_logic_extractor:extract_logic:152 - ✅ Extracted 2 steps from Ab Initio graph: 1500_CDD_TUSourcedFamilyMemberLink       
2025-11-19 20:55:05.887 | INFO     | services.stag.stag_orchestrator:generate_comparison:178 -
📊 Step 3: Extracting Databricks logic...
2025-11-19 20:55:05.887 | INFO     | services.stag.databricks_logic_extractor:extract_logic:66 - 📊 Extracting logic for Databricks pipeline: pl_TUSourcedFamilyMemberLink
2025-11-19 20:55:05.948 | ERROR    | services.local_search.local_search_client:search:323 - Error searching: Error executing plan: Internal error: Error creating hnsw segment reader: Nothing found on disk
2025-11-19 20:55:05.948 | INFO     | services.stag.databricks_logic_extractor:_search_pipeline_documents:122 -    Found 0 Databricks/ADF documents for pl_TUSourcedFamilyMemberLink 
2025-11-19 20:55:05.948 | WARNING  | services.stag.databricks_logic_extractor:extract_logic:72 - No documents found for pipeline: pl_TUSourcedFamilyMemberLink
2025-11-19 20:55:05.949 | WARNING  | services.stag.stag_orchestrator:generate_comparison:182 -    ⚠ No logic extracted for pl_TUSourcedFamilyMemberLink (may not be indexed)        
2025-11-19 20:55:05.949 | INFO     | services.stag.stag_orchestrator:generate_comparison:185 -
🤖 Step 4: Abstracting to business stages (AI)...
2025-11-19 20:55:05.949 | INFO     | services.stag.business_stage_abstractor:abstract_to_business_stages:60 - 🤖 Abstracting technical logic to business stages using AI
2025-11-19 20:55:05.950 | INFO     | services.stag.business_stage_abstractor:_generate_business_stages_with_rag:202 -    🤖 Sending RAG prompt to AI analyzer...
2025-11-19 20:55:09.737 | INFO     | services.stag.business_stage_abstractor:_parse_ai_response_to_stages:370 -    ✅ Parsed 3 stages from AI response
2025-11-19 20:55:09.737 | INFO     | services.stag.business_stage_abstractor:_validate_and_enrich_stages:427 -    ✅ Validated 3/3 stages
2025-11-19 20:55:09.737 | INFO     | services.stag.business_stage_abstractor:abstract_to_business_stages:73 - ✅ Generated 3 business stages
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_orchestrator:generate_comparison:197 -    ✅ Generated 3 business stages (3 differences)
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_orchestrator:generate_comparison:200 -
🔗 Step 5: Generating STTM (AI)...
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:generate_sttm:116 - 🔗 Generating STTM from abinitio to Databricks (using code-extracted schemas)
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:_extract_schema_from_code:206 -    📋 Extracting schema from CODE for abinitio
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:_extract_schema_from_code:259 -       ✅ Extracted 0 unique columns from 0 source files
2025-11-19 20:55:09.737 | WARNING  | services.stag.stag_sttm_generator:_extract_schema_from_code:262 -       ⚠ No columns extracted from abinitio - column_schemas may be empty!    
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:_extract_schema_from_code:206 -    📋 Extracting schema from CODE for databricks
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:_extract_schema_from_code:259 -       ✅ Extracted 0 unique columns from 0 source files
2025-11-19 20:55:09.737 | WARNING  | services.stag.stag_sttm_generator:_extract_schema_from_code:262 -       ⚠ No columns extracted from databricks - column_schemas may be empty!  
2025-11-19 20:55:09.737 | WARNING  | services.stag.stag_sttm_generator:generate_sttm:142 -    ⚠ Code-extracted schemas incomplete: source=0 cols, target=0 cols
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:generate_sttm:150 -    📋 Source schema: 0 columns
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:generate_sttm:151 -    📋 Target schema: 0 columns
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:_generate_column_mappings_with_ai:553 -    🤖 Generating AI-based column mappings (grounded in extracted schemas)
2025-11-19 20:55:09.737 | WARNING  | services.stag.stag_sttm_generator:_generate_column_mappings_with_ai:559 -       ⚠ No columns in either source or target schema!
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_sttm_generator:generate_sttm:166 - ✅ Generated 0 column mappings
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_orchestrator:generate_comparison:208 -    ✅ Generated 0 column mappings
2025-11-19 20:55:09.737 | INFO     | services.stag.stag_orchestrator:generate_comparison:211 -
📄 Step 6: Generating Excel comparison report...
2025-11-19 20:55:09.737 | INFO     | services.stag.excel_generator:generate_comparison_excel:86 - 📊 Generating Excel comparison: 1500_CDD_TUSourcedFamilyMemberLink → pl_TUSourcedFamilyMemberLink
2025-11-19 20:55:09.737 | INFO     | services.stag.excel_generator:generate_comparison_excel:96 -    Creating Overview sheet...
2025-11-19 20:55:09.752 | INFO     | services.stag.excel_generator:generate_comparison_excel:99 -    Creating Databricks Logic sheet...
2025-11-19 20:55:09.753 | INFO     | services.stag.excel_generator:generate_comparison_excel:102 -    Creating Abinitio Logic sheet...
2025-11-19 20:55:09.754 | INFO     | services.stag.excel_generator:generate_comparison_excel:105 -    Creating Logic Comparison sheet...
2025-11-19 20:55:09.755 | INFO     | services.stag.excel_generator:_build_logic_comparison:576 -    Comparing 2 abinitio items with 0 Databricks items
2025-11-19 20:55:09.755 | INFO     | services.stag.excel_generator:_build_logic_comparison:615 -    ✅ Generated 2 comparison rows
2025-11-19 20:55:09.756 | INFO     | services.stag.excel_generator:generate_comparison_excel:108 -    Creating STTM sheets (Source, Target, Comparison)...
2025-11-19 20:55:09.757 | INFO     | services.stag.excel_generator:_create_databricks_sttm_section:1002 - Creating Databricks STTM section...
2025-11-19 20:55:09.758 | INFO     | services.stag.excel_generator:_create_databricks_sttm_section:1082 -    Databricks STTM section: 1 column rows
2025-11-19 20:55:09.758 | INFO     | services.stag.excel_generator:_create_source_sttm_section:1098 - Creating abinitio STTM section...
2025-11-19 20:55:09.760 | INFO     | services.stag.excel_generator:_create_source_sttm_section:1177 -    abinitio STTM section: 1 column rows
2025-11-19 20:55:09.760 | INFO     | services.stag.excel_generator:_create_sttm_comparison_section:1198 - Creating STTM Comparison section...
2025-11-19 20:55:09.761 | INFO     | services.stag.excel_generator:_create_sttm_comparison_section:1250 -    Comparison section: 0 comparison rows
2025-11-19 20:55:09.886 | INFO     | services.stag.excel_generator:generate_comparison_excel:126 - ✅ Excel file saved: outputs/stag_comparisons\1500_CDD_TUSourcedFamilyMemberLink_vs_pl_TUSourcedFamilyMemberLink_20251119_205509.xlsx        
2025-11-19 20:55:09.887 | INFO     | services.stag.stag_orchestrator:generate_comparison:227 -    ✅ Excel file generated: outputs/stag_comparisons\1500_CDD_TUSourcedFamilyMemberLink_vs_pl_TUSourcedFamilyMemberLink_20251119_205509.xlsx     
2025-11-19 20:55:09.887 | INFO     | services.stag.stag_orchestrator:generate_comparison:241 -
================================================================================
2025-11-19 20:55:09.887 | INFO     | services.stag.stag_orchestrator:generate_comparison:242 - ✅ STAG COMPARISON COMPLETE
2025-11-19 20:55:09.888 | INFO     | services.stag.stag_orchestrator:generate_comparison:243 - ================================================================================     
2025-11-19 20:55:09.888 | INFO     | services.stag.stag_orchestrator:generate_comparison:244 - Source: abinitio: 1500_CDD_TUSourcedFamilyMemberLink
2025-11-19 20:55:09.888 | INFO     | services.stag.stag_orchestrator:generate_comparison:245 - Databricks Pipelines Processed: 1
2025-11-19 20:55:09.888 | INFO     | services.stag.stag_orchestrator:generate_comparison:247 -   1. pl_TUSourcedFamilyMemberLink
2025-11-19 20:55:09.888 | INFO     | services.stag.stag_orchestrator:generate_comparison:248 -      - Business Stages: 3
2025-11-19 20:55:09.889 | INFO     | services.stag.stag_orchestrator:generate_comparison:249 -      - STTM Mappings: 0  
2025-11-19 20:55:09.889 | INFO     | services.stag.stag_orchestrator:generate_comparison:250 -      - Differences: 3    
2025-11-19 20:55:09.889 | INFO     | services.stag.stag_orchestrator:generate_comparison:251 -      - Excel: outputs/stag_comparisons\1500_CDD_TUSourcedFamilyMemberLink_vs_pl_TUSourcedFamilyMemberLink_20251119_205509.xlsx
2025-11-19 20:55:09.889 | INFO     | services.stag.stag_orchestrator:generate_comparison:252 -
Totals Across All Pipelines:
2025-11-19 20:55:09.889 | INFO     | services.stag.stag_orchestrator:generate_comparison:253 -   - Business Stages: 3   
2025-11-19 20:55:09.890 | INFO     | services.stag.stag_orchestrator:generate_comparison:254 -   - STTM Mappings: 0     
2025-11-19 20:55:09.890 | INFO     | services.stag.stag_orchestrator:generate_comparison:255 -   - Differences: 3       
2025-11-19 20:55:09.890 | INFO     | services.stag.stag_orchestrator:generate_comparison:256 - ================================================================================  
