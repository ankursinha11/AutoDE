================================================================================
🎯 Extracting detailed hierarchy for Graph ID: 1
================================================================================

📂 Loading subgraph JSON from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\265_fileTransferToHadoopServer_components.json
✅ Loaded 0 subgraphs from JSON
🔍 Processing graph ID: 1 (Level 0)
❌ Graph ID 1 not found in subgraphs
2025-11-19 18:10:02.641 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step1:160 - Step 1 execution failed (likely format mismatch), using enhanced extraction: Graph ID 1 not found or could not be processed
2025-11-19 18:10:02.648 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:235 - Enhanced step 1 complete with raw_content: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_detailed_graph1.json
2025-11-19 18:10:02.648 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:236 -   Vertices: 14, Raw content size: 59412 chars
2025-11-19 18:10:02.648 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:100 - Step 2: Embedding DML/XFR content...
🔧 Initializing GPT-5 LLM...
✅ GPT-5 LLM initialized (Model: gpt-5)

📂 Loading files from: Input Files\blade (including all subfolders)

✅ Total files loaded: 0
   Available files: []


================================================================================
STEP 2: EMBED DML/XFR FILES (HIERARCHICAL PROCESSING)
================================================================================

pServer_detailed_graph1.json
✅ Loaded successfully!

🔄 Starting hierarchical processing (deepest level first)...


============================================================
📊 Graph 1: 265_fileTransferToHadoopServer (Level 0)
============================================================

🔄 Processing Graph 1...
   Vertices: 14
   🤖 Calling GPT-5 to extract files from Graph 1...
   📄 Found 3 unique file(s): ['error-info.dml', 'PatientAccts.dml', 'log-info.dml']
      ⚠️  error-info.dml NOT FOUND in data/ folder
      ⚠️  PatientAccts.dml NOT FOUND in data/ folder
      ⚠️  log-info.dml NOT FOUND in data/ folder

✅ Graph 1 processing complete!

================================================================================
💾 Saving to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_detailed_graph1_with_files.json
✅ Saved successfully! File size: 105.85 KB

================================================================================
📊 SUMMARY
================================================================================
Total file references embedded: 0
Output file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_detailed_graph1_with_files.json
================================================================================

✅ STEP 2 COMPLETE!

2025-11-19 18:10:26.080 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:271 - Step 2 complete: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_detailed_graph1_with_files.json
2025-11-19 18:10:26.080 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5...
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
📖 Loading graph data from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_detailed_graph1_with_files.json
✅ Loaded successfully!

📊 Main Graph: 265_fileTransferToHadoopServer (ID: 1)
📊 Total graphs in hierarchy: 1
📊 Subgraphs to process: 0
   Processing order: []

================================================================================
PHASE 2: PROCESS SUBGRAPHS (BOTTOM-UP)
         Extract: detailed functional logic (inputs, transformations, outputs)
================================================================================

💾 Saving subgraph logic to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_subgraph_logic_optimized.json
✅ Saved successfully! File size: 0.00 KB

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
   📊 Response length: 153 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Identified 1 output(s):
      1. fvertex_32 (ID: 32)

================================================================================
📋 PHASE 2.7: GENERATE MAIN GRAPH SUMMARY
================================================================================
   🤖 Calling GPT-5 LLM to generate graph summary...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\graph_summary_response.txt
   📊 Response length: 1008 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Graph summary generated successfully
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
🔄 PHASE 3: GENERATE MAPPINGS FOR EACH OUTPUT
================================================================================

================================================================================
📋 Processing Output 1/1: fvertex_32 (ID: 32)
================================================================================
   📝 Prompt saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_1_prompt.txt (Length: 113,387 chars)
   🤖 Calling GPT-5 LLM for attribute mapping...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_1_response.txt
   📊 Response length: 609 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Generated 1 attribute mapping(s) for fvertex_32

================================================================================
✅ MAIN GRAPH PROCESSING COMPLETE
   Total outputs processed: 1
================================================================================

💾 Saving final mapping to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_final_mapping_optimized.json
✅ Saved successfully! File size: 1.88 KB

================================================================================
PHASE 4: GENERATE EXCEL OUTPUT
================================================================================

📊 Generating Excel file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_source_to_target_mapping.xlsx
   📄 Creating Summary sheet
   📄 Creating sheet: fvertex_32
   ✅ Added 1 mapping entries
✅ Excel file saved! File size: 6.38 KB

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

2025-11-19 18:13:38.406 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:321 - Excel file generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_source_to_target_mapping.xlsx
2025-11-19 18:13:38.406 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:325 - Mapping JSON generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_final_mapping_optimized.json
2025-11-19 18:13:38.406 | INFO     | __main__:index_all_repository_files_with_ai:1799 - 📋 Generated STTM: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServer_source_to_target_mapping.xlsx  
2025-11-19 18:13:44.386 | WARNING  | __main__:index_all_repository_files_with_ai:1855 - ⚠️ Truncating RAW mp file content for 265_fileeTransferToHadoopServer.mp from 59410 to 50000 chars
2025-11-19 18:13:44.386 | INFO     | __main__:index_all_repository_files_with_ai:1856 -    ℹ️  Note: PARSED components (vertices/flowss/ports) are FULLY embedded, only raw mp content truncated
2025-11-19 18:13:44.386 | INFO     | __main__:index_all_repository_files_with_ai:1973 -    📦 Embedded in vector DB: 14 vertices, 5 flows, 38 ports
2025-11-19 18:13:44.386 | INFO     | __main__:index_all_repository_files_with_ai:1974 -    📏 Document size: 58,400 chars (parsed components: 2,817 chars)
2025-11-19 18:13:44.386 | DEBUG    | __main__:index_all_repository_files_with_ai:1994 - ✓ Created document for 265_fileTransferToHadoopServer.mp (total: 1)
🔍 ENHANCED PARSING - EXTRACTING ALL COMPONENTS
============================================================       
   📊 Extracting graphs with fixed name parsing...
      • Found 1 graphs with corrected names
