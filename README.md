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
