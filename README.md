ers\abinitio\automation\step2_embed_dml_xfr.py)
2025-11-19 15:20:00.511 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5...
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
📖 Loading graph data from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_detailed_graph1.json
✅ Loaded successfully!

2025-11-19 15:20:00.531 | ERROR    | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:310 - Step 3 failed: 'graph'
2025-11-19 15:20:06.415 | WARNING  | __main__:index_all_repository_files_with_ai:1831 - ⚠️ Truncating large file 265_fileTransferToHadoopServerAdhoc_remove.mp ffrom 1508448 to 50000 chars
2025-11-19 15:20:06.419 | DEBUG    | __main__:index_all_repository_files_with_ai:1963 - ✓ Created document for 265_fileTransferToHadoopServerAdhoc_remove.mp (total: 8)
🔍 ENHANCED PARSING - EXTRACTING ALL COMPONENTS
============================================================
   📊 Extracting graphs with fixed name parsing...
      • Found 47 graphs with corrected names

📊 FLOW TYPE ANALYSIS:
   Total flows: 291
   Flow connections: 582
   📋 FLOW TYPE DISTRIBUTION:
      • PARAMETER_FLOW: 291
   📋 CONFIG FLOWS IDENTIFIED: 291

✅ ENHANCED PARSING COMPLETE:
   📊 COMPONENT SUMMARY:
      • Total Graphs: 47
      • Total Vertices: 455
      • Total Flows: 291
      • Total Ports: 2076
      • Total Flow Connections: 582
      • Total Port Bindings: 952
      • Total Config Flows: 291
      • Total Graph Vertex Links: 371
      • Total Graph Flow Links: 291

💾 Enhanced components saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\265_fileTransferToHadoopServerAdhoc_SplitBatches_components.json
2025-11-19 15:20:07.329 | INFO     | __main__:index_all_repository_files_with_ai:1745 - ✅ Parsed 265_fileTransferToHadoopServerAdhoc_SplitBatches.mp: 455 vertices, 291 flows, 2076 ports
2025-11-19 15:20:07.329 | INFO     | __main__:index_all_repository_files_with_ai:1746 -    Saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\265_fileTransferToHadoopServerAdhoc_SplitBatches_components.json
2025-11-19 15:20:07.715 | INFO     | __main__:index_all_repository_files_with_ai:1759 - 📊 Generated GraphFlow: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\graphflows\265_fileTransferToHadoopServerAdhoc_SplitBatches_graphflow.xlsx
2025-11-19 15:20:07.716 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:85 - Generating STTM for: 265_fileTransferToHadoopServerAdhoc_SplitBatches
2025-11-19 15:20:07.716 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:86 - Output folder: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation
2025-11-19 15:20:07.717 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:90 - Step 1: Extracting graph details...
2025-11-19 15:20:07.717 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step1:160 - Step 1 import failed, using simplified extraction: cannot import name 'GraphDetailExtractor' from 'parsers.abinitio.automation.step1_extract_graph1_details' (C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\automation\step1_extract_graph1_details.py)
2025-11-19 15:20:07.753 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:205 - Simplified step 1 complete: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_SplitBatches_detailed_graph1.json
2025-11-19 15:20:07.755 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:100 - Step 2: Embedding DML/XFR content...
2025-11-19 15:20:07.755 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:249 - Step 2 import failed, skipping DML embedding: cannot import name 'DMLXFREmbed' from 'parsers.abinitio.automation.step2_embed_dml_xfr' (C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\automation\step2_embed_dml_xfr.py)
2025-11-19 15:20:07.755 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5...
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
📖 Loading graph data from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_SplitBatches_detailed_graph1.json
✅ Loaded successfully!

2025-11-19 15:20:07.775 | ERROR    | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:310 - Step 3 failed: 'graph'
  Stopping...
2025-11-19 15:20:13.509 | WARNING  | __main__:index_all_repository_files_with_ai:1831 - ⚠️ Truncating large file 265_fileTransferToHadoopServerAdhoc_SplitBatchees.mp from 1636497 to 50000 chars
2025-11-19 15:20:13.509 | DEBUG    | __main__:index_all_repository_files_with_ai:1963 - ✓ Created document for 265_fileTransferToHadoopServerAdhoc_SplitBatches.mp (total: 9)
