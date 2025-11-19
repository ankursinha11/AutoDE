 STAG Orchestrator initialized
2025-11-19 17:17:17.721 | INFO     | services.chat.chat_orchestrator:__init__:114 - ChatOrchestrator initialized with 5 specialized agents + LogicComparator + Codebase Copilot + Document Tools + STAG Orchestrator
2025-11-19 17:17:18.054 | INFO     | __main__:initialize_rag_components:257 - ✓ STAG RAG components initialized
2025-11-19 17:17:18.522 Please replace `use_container_width` with `width`.

`use_container_width` will be removed after 2025-12-31.

For `use_container_width=True`, use `width='stretch'`. For `use_container_width=False`, use `width='content'`.
2025-11-19 17:17:19.123 | INFO     | services.ai_script_analyzer:__init__:48 - ✓ AI Script Analyzer initialized with gpt-4
2025-11-19 17:17:19.123 | INFO     | services.lineage.lineage_agents:__init__:68 - ✓ Parsing Agent initialized with cross-system context support
2025-11-19 17:17:19.123 | INFO     | services.lineage.lineage_agents:__init__:446 - ✓ Logic Agent initialized
2025-11-19 17:17:19.124 | INFO     | services.lineage.sttm_generator:__init__:87 - ✓ STTM Generator initialized
2025-11-19 17:17:19.124 | INFO     | services.lineage.lineage_agents:__init__:696 - ✓ Mapping Agent initialized
2025-11-19 17:17:19.673 | INFO     | services.logic_comparator:__init__:49 - ✓ Logic Comparator initialized with Azure OpenAI
2025-11-19 17:17:19.673 | INFO     | services.lineage.lineage_agents:__init__:761 - ✓ Similarity Agent initialized
2025-11-19 17:17:19.673 | INFO     | services.lineage.lineage_agents:__init__:954 - ✓ Lineage Agent initialized
2025-11-19 17:17:19.673 | INFO     | services.lineage.lineage_agents:__init__:1086 - ✓ Lineage Orchestrator initialized with all agents (cross-system context enabled)
2025-11-19 17:17:19.674 | INFO     | ui.lineage_tab:render_lineage_tab:80 - ✓ Lineage Orchestrator initialized
2025-11-19 17:17:19.680 | INFO     | services.lineage.lineage_agents:__init__:761 - ✓ Similarity Agent initialized
2025-11-19 17:17:19.687 | INFO     | services.metadata_extractor:get_tables:50 - 📊 Extracting tables from hadoop...
2025-11-19 17:17:20.036 | INFO     | services.metadata_extractor:get_tables:79 -   ✓ Found 375 unique tables in hadoop
2025-11-19 17:17:45.153 Please replace `use_container_width` with `width`.

`use_container_width` will be removed after 2025-12-31.

For `use_container_width=True`, use `width='stretch'`. For `use_container_width=False`, use `width='content'`.
2025-11-19 17:17:55.923 Please replace `use_container_width` with `width`.

`use_container_width` will be removed after 2025-12-31.

For `use_container_width=True`, use `width='stretch'`. For `use_container_width=False`, use `width='content'`.
2025-11-19 17:17:55.947 | INFO     | services.lineage.sttm_generator:__init__:87 - ✓ STTM Generator initialized
2025-11-19 17:17:55.948 | INFO     | __main__:reindex_abinitio_from_directory:2205 - ✓ STTM Generator initialized
2025-11-19 17:17:56.415 | INFO     | __main__:index_all_repository_files_with_ai:1629 - Filtered from 1750 to 260 files using 36 graph patterns
2025-11-19 17:17:56.418 | INFO     | __main__:index_all_repository_files_with_ai:1664 - Output directory: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\ai_enriched_docs\abinitio
2025-11-19 17:17:56.418 | INFO     | __main__:index_all_repository_files_with_ai:1665 - STTM directory: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_mappings\abinitio
2025-11-19 17:17:56.418 | INFO     | __main__:index_all_repository_files_with_ai:1668 - AI Analyzer enabled: True
2025-11-19 17:17:56.419 | INFO     | __main__:index_all_repository_files_with_ai:1669 - STTM Generator available: True
2025-11-19 17:17:56.421 | INFO     | __main__:index_all_repository_files_with_ai:1681 - Parsed AbInitio output folder: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio
2025-11-19 17:17:56.422 | INFO     | __main__:index_all_repository_files_with_ai:1682 - GraphFlow output folder: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\graphflows
2025-11-19 17:17:56.422 | INFO     | __main__:index_all_repository_files_with_ai:1683 - STTM Automation output folder: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation
2025-11-19 17:17:56.422 | INFO     | __main__:index_all_repository_files_with_ai:1695 - ✅ EnhancedAbInitioParser initialized for structured component extraction
2025-11-19 17:17:56.422 | INFO     | __main__:index_all_repository_files_with_ai:1704 - ✅ GraphFlowGenerator initialized for visualization
2025-11-19 17:17:56.423 | INFO     | __main__:index_all_repository_files_with_ai:1717 - ✅ AbInitioSTTMGenerator initialized for STTM automation
🔍 ENHANCED PARSING - EXTRACTING ALL COMPONENTS
============================================================       
   📊 Extracting graphs with fixed name parsing...
      • Found 1 graphs with corrected names

📊 FLOW TYPE ANALYSIS:
   Total flows: 5
   Flow connections: 10
   📋 FLOW TYPE DISTRIBUTION:
      • PARAMETER_FLOW: 5
   📋 CONFIG FLOWS IDENTIFIED: 5

✅ ENHANCED PARSING COMPLETE:
   📊 COMPONENT SUMMARY:
      • Total Graphs: 1
      • Total Vertices: 14
      • Total Flows: 5
      • Total Ports: 38
      • Total Flow Connections: 10
      • Total Port Bindings: 0
      • Total Config Flows: 5
      • Total Graph Vertex Links: 7
      • Total Graph Flow Links: 5

💾 Enhanced components saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\265_fileTransferToHadoopServer_components.json
2025-11-19 17:17:56.442 | INFO     | __main__:index_all_repository_files_with_ai:1769 - ✅ Parsed 265_fileTransferToHadoopServer.mp: 14 vertices, 5 flows, 38 ports
2025-11-19 17:17:56.443 | INFO     | __main__:index_all_repository_files_with_ai:1770 -    Saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\265_fileTransferToHadoopServer_components.json
2025-11-19 17:17:56.526 | INFO     | __main__:index_all_repository_files_with_ai:1783 - 📊 Generated GraphFlow: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\graphflows\265_fileTransferToHadoopServer_graphflow.xlsx
2025-11-19 17:17:56.527 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:85 - Generating STTM for: 265_fileTransferToHadoopServer
2025-11-19 17:17:56.528 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:86 - Output folder: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation
2025-11-19 17:17:56.528 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:90 - Step 1: Extracting graph details...
2025-11-19 17:17:56.531 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step1:160 - Step 1 import failed, using simplified extraction: cannot import name 'GraphDetailExtractor' from 'parsers.abinitio.automation.step1_extract_graph1_details' (C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\automation\step1_extract_graph1_details.py)   
2025-11-19 17:17:56.537 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:221 - Simplified step 1 complete: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_detailed_graph1.json
2025-11-19 17:17:56.537 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:100 - Step 2: Embedding DML/XFR content...
2025-11-19 17:17:56.539 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:265 - Step 2 import failed, skipping DML embedding: cannot import name 'DMLXFREmbed' from 'parsers.abinitio.automation.step2_embed_dml_xfr' (C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\automation\step2_embed_dml_xfr.py)
2025-11-19 17:17:56.539 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5...
🔧 Using STAG's AI analyzer for STTM generation...
✅ AI analyzer initialized


╔════════════════════════════════════════════════════════════════════════════╗
║    STEP 3: SOURCE-TO-TARGET ATTRIBUTE MAPPING GENERATOR (HYBRID)          ║
║                                                                  
          ║
║  Processing Strategy:                                            
          ║
║    - Phase 0: DML Optimization (LLM identifies required sections)         ║
║    - Phase 1: Load graph data & build dependency tree            
         ║
║    - Phase 2: Extract detailed functional logic from subgraphs            ║
║    - Phase 2.5: Identify all output components (LLM-driven)               ║
║    - Phase 3: Process EACH output separately (one LLM call per output)    ║
║    - Phase 4: Generate Excel with all outputs                    
         ║
║  Flow: SOURCE → TARGET (trace forward from inputs to outputs)             ║
║  Output: Excel file with detailed attribute-level mapping + datatypes      ║
╚════════════════════════════════════════════════════════════════════════════╝


================================================================================
PHASE 1: LOAD GRAPH DATA & BUILD DEPENDENCY TREE
================================================================================
📖 Loading graph data from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_detailed_graph1.json
✅ Loaded successfully!

📊 Main Graph: 265_fileTransferToHadoopServer (ID: 1)
📊 Total graphs in hierarchy: 1
📊 Subgraphs to process: 0
   Processing order: []

================================================================================
PHASE 2: PROCESS SUBGRAPHS (BOTTOM-UP)
         Extract: detailed functional logic (inputs, transformations, outputs)
================================================================================

================================================================================

================================================================================
PHASE 3: PROCESS MAIN GRAPH (HYBRID OPTIMIZATION)
         - DML reduction to required sections only
         - Identify all outputs dynamically
         - Generate mapping for each output separately
================================================================================

================================================================================
🎯 Processing Main Graph 1: 265_fileTransferToHadoopServer
================================================================================
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
📦 PHASE 0: CONTEXT-AWARE DML OPTIMIZATION
================================================================================
   ℹ️  No referenced files to optimize
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
🔍 PHASE 2.5: IDENTIFY OUTPUT COMPONENTS
================================================================================
   🤖 Asking LLM to identify all outputs...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\identify_outputs_response.txt
   📊 Response length: 41 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Identified 0 output(s):
   ⚠️  No outputs identified, cannot generate mappings

💾 Saving final mapping to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_final_mapping_optimized.json
✅ Saved successfully! File size: 0.14 KB

================================================================================
PHASE 4: GENERATE EXCEL OUTPUT
================================================================================

📊 Generating Excel file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_source_to_target_mapping.xlsx
   📄 Creating Summary sheet
   ⚠️  No outputs found in mapping
✅ Excel file saved! File size: 5.50 KB

================================================================================
📋 EXECUTION SUMMARY
================================================================================
Main Graph ID: 1
Main Graph Name: 265_fileTransferToHadoopServer
Subgraphs Processed: 0
Output Files Generated:
  - 265_fileTransferToHadoopServer_subgraph_logic_optimized.json
  - 265_fileTransferToHadoopServer_final_mapping_optimized.json
  - 265_fileTransferToHadoopServer_source_to_target_mapping.xlsx
All files saved in: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation
================================================================================

✅ STEP 3 COMPLETE!

2025-11-19 17:19:57.464 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:311 - Excel file generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_source_to_target_mapping.xlsx
2025-11-19 17:19:57.464 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:315 - Mapping JSON generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_final_mapping_optimized.json
2025-11-19 17:19:57.465 | INFO     | __main__:index_all_repository_files_with_ai:1799 - 📋 Generated STTM: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_source_to_target_mapping.xlsx
2025-11-19 17:20:06.120 | WARNING  | __main__:index_all_repository_files_with_ai:1855 - ⚠️ Truncating RAW mp file content for 265_fileTransferToHadoopServer.mp from 559410 to 50000 chars
2025-11-19 17:20:06.120 | INFO     | __main__:index_all_repository_files_with_ai:1856 -    ℹ️  Note: PARSED components (vertices/flows/ports) are FULLY embedded, onlyy raw mp content truncated
2025-11-19 17:20:06.121 | INFO     | __main__:index_all_repository_files_with_ai:1973 -    📦 Embedded in vector DB: 14 vertices, 5 flows, 38 ports
2025-11-19 17:20:06.121 | INFO     | __main__:index_all_repository_files_with_ai:1974 -    📏 Document size: 59,597 chars (parsed components: 2,817 chars)
2025-11-19 17:20:06.121 | DEBUG    | __main__:index_all_repository_files_with_ai:1994 - ✓ Created document for 265_fileTransferToHadoopServer.mp (total: 1)
🔍 ENHANCED PARSING - EXTRACTING ALL COMPONENTS
============================================================
   📊 Extracting graphs with fixed name parsing...
      • Found 1 graphs with corrected names

📊 FLOW TYPE ANALYSIS:
   Total flows: 17
   Flow connections: 34
   📋 FLOW TYPE DISTRIBUTION:
      • PARAMETER_FLOW: 17
   📋 CONFIG FLOWS IDENTIFIED: 17

✅ ENHANCED PARSING COMPLETE:
   📊 COMPONENT SUMMARY:
      • Total Graphs: 1
      • Total Vertices: 36
      • Total Flows: 17
      • Total Ports: 106
      • Total Flow Connections: 34
      • Total Port Bindings: 0
      • Total Config Flows: 17
      • Total Graph Vertex Links: 18
      • Total Graph Flow Links: 17

💾 Enhanced components saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\265_fileTransferToHadoopServer_3_components.json
2025-11-19 17:20:06.155 | INFO     | __main__:index_all_repository_files_with_ai:1769 - ✅ Parsed 265_fileTransferToHadoopServer_3.mp: 36 vertices, 17 flows, 106 ports
2025-11-19 17:20:06.156 | INFO     | __main__:index_all_repository_files_with_ai:1770 -    Saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\265_fileTransferToHadoopServer_3_components.json
2025-11-19 17:20:06.284 | INFO     | __main__:index_all_repository_files_with_ai:1783 - 📊 Generated GraphFlow: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\graphflows\265_fileTransferToHadoopServer_3_graphflow.xlsx
2025-11-19 17:20:06.285 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:85 - Generating STTM for: 265_fileTransferToHadoopServer_3
2025-11-19 17:20:06.285 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:86 - Output folder: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation
2025-11-19 17:20:06.286 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:90 - Step 1: Extracting graph details...      
2025-11-19 17:20:06.286 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step1:160 - Step 1 import failed, using simplified extraction: cannot import name 'GraphDetailExtractor' from 'parsers.abinitio.automation.step1_extract_graph1_details' (C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\automation\step1_extract_graph1_details.py)
2025-11-19 17:20:06.291 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:221 - Simplified step 1 complete: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_3_detailed_graph1.json
2025-11-19 17:20:06.291 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:100 - Step 2: Embedding DML/XFR content...    
2025-11-19 17:20:06.291 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:265 - Step 2 import failed, skipping DML embedding: cannot import name 'DMLXFREmbed' from 'parsers.abinitio.automation.step2_embed_dml_xfr' (C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\automation\step2_embed_dml_xfr.py)
2025-11-19 17:20:06.291 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5...
🔧 Using STAG's AI analyzer for STTM generation...
✅ AI analyzer initialized


╔════════════════════════════════════════════════════════════════════════════╗
║    STEP 3: SOURCE-TO-TARGET ATTRIBUTE MAPPING GENERATOR (HYBRID)          ║
║                                                                            ║
║  Processing Strategy:                                                      ║
║    - Phase 0: DML Optimization (LLM identifies required sections)         ║
║    - Phase 1: Load graph data & build dependency tree                     ║
║    - Phase 2: Extract detailed functional logic from subgraphs            ║
║    - Phase 2.5: Identify all output components (LLM-driven)               ║
║    - Phase 3: Process EACH output separately (one LLM call per output)    ║
║    - Phase 4: Generate Excel with all outputs                             ║
║  Flow: SOURCE → TARGET (trace forward from inputs to outputs)             ║
║  Output: Excel file with detailed attribute-level mapping + datatypes      ║
╚════════════════════════════════════════════════════════════════════════════╝


================================================================================
PHASE 1: LOAD GRAPH DATA & BUILD DEPENDENCY TREE
================================================================================
📖 Loading graph data from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_3_detailed_graph1.json
✅ Loaded successfully!

📊 Main Graph: 265_fileTransferToHadoopServer_3 (ID: 1)
📊 Total graphs in hierarchy: 1
📊 Subgraphs to process: 0
   Processing order: []

================================================================================
PHASE 2: PROCESS SUBGRAPHS (BOTTOM-UP)
         Extract: detailed functional logic (inputs, transformations, outputs)
================================================================================

💾 Saving subgraph logic to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_3_subgraph_logic_optimized.json
✅ Saved successfully! File size: 0.00 KB

================================================================================
PHASE 3: PROCESS MAIN GRAPH (HYBRID OPTIMIZATION)
         - DML reduction to required sections only
         - Identify all outputs dynamically
         - Generate mapping for each output separately
================================================================================

================================================================================
🎯 Processing Main Graph 1: 265_fileTransferToHadoopServer_3
================================================================================
   ⏳ Waiting 60 seconds to avoid rate limit...
