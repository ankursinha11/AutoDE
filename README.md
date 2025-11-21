2025-11-21 11:54:44.265 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Input_File
2025-11-21 11:54:58.783 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 25 steps
2025-11-21 11:54:58.783 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Input_File
2025-11-21 11:55:13.130 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 28 steps
2025-11-21 11:55:13.130 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Input_File
2025-11-21 11:55:22.824 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 26 steps
2025-11-21 11:55:22.824 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Input_File
2025-11-21 11:55:30.916 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 24 steps
2025-11-21 11:55:30.916 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Input_File
2025-11-21 11:55:41.153 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 30 steps
2025-11-21 11:55:41.154 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Input_File
2025-11-21 11:55:50.407 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 25 steps
2025-11-21 11:55:50.407 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: IFileTUSourcedFamilyMemberLink  
2025-11-21 11:55:59.812 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 25 steps
2025-11-21 11:55:59.812 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: IFilePatientAcctsXRefPermID     
2025-11-21 11:56:10.990 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 27 steps
2025-11-21 11:56:10.992 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: IFileTU_eScan_PatientAcct_PermIDMappingRaw
2025-11-21 11:56:19.522 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 26 steps
2025-11-21 11:56:19.523 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: LkpHospitals
2025-11-21 11:56:29.667 | DEBUG    | services.stag.abinitio_logic_extractor:_parse_component_analysis_response:454 - Failed to parse AI component analysis: Expecting ',' delimiter: line 39 column 52 (char 4842)
2025-11-21 11:56:29.667 | WARNING  | services.stag.abinitio_logic_extractor:_analyze_component_deeply:426 -    ⚠ AI analysis incomplete for LkpHospitals
2025-11-21 11:56:29.667 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: OFile_ediTUSourcedFamilyMemberLink.dat
2025-11-21 11:56:38.860 | DEBUG    | services.stag.abinitio_logic_extractor:_parse_component_analysis_response:454 - Failed to parse AI component analysis: Expecting ',' delimiter: line 38 column 47 (char 4508)
2025-11-21 11:56:38.862 | WARNING  | services.stag.abinitio_logic_extractor:_analyze_component_deeply:426 -    ⚠ AI analysis incomplete for OFile_ediTUSourcedFamilyMemberLink.dat
2025-11-21 11:56:38.862 | INFO     | services.stag.abinitio_logic_extractor:_extract_steps_from_components:333 -    ✅ Deeply analyzed 1214 components from Ab Initio graph 
2025-11-21 11:56:38.870 | INFO     | services.stag.abinitio_logic_extractor:extract_logic:124 -    🔄 Generating detailed STTM using VM_Automation...
2025-11-21 11:56:38.872 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:85 - Generating STTM for: 1500_CDD_TUSourcedFamilyMemberLink
2025-11-21 11:56:38.872 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:86 - Output folder: outputs\sttm_abinitio
2025-11-21 11:56:38.872 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:90 - Step 1: Extracting graph details...

================================================================================      
🎯 Extracting detailed hierarchy for Graph ID: 1
================================================================================      

📂 Loading subgraph JSON from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\1500_CDD_TUSourcedFamilyMemberLink_components.json
✅ Loaded 0 subgraphs from JSON
🔍 Processing graph ID: 1 (Level 0)
❌ Graph ID 1 not found in subgraphs
2025-11-21 11:56:38.925 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step1:160 - Step 1 execution failed (likely format mismatch), using enhanced extraction: Graph ID 1 not found or could not be processed
2025-11-21 11:56:38.995 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:235 - Enhanced step 1 complete with raw_content: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1.json
2025-11-21 11:56:38.995 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:236 -   Vertices: 1214, Raw content size: 10000 chars
2025-11-21 11:56:38.998 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:100 - Step 2: Embedding DML/XFR content...
🔧 Initializing GPT-5 LLM...
✅ GPT-5 LLM initialized (Model: gpt-5)

📂 Loading files from: Input Files\blade (including all subfolders)

✅ Total files loaded: 0
   Available files: []


================================================================================      
STEP 2: EMBED DML/XFR FILES (HIERARCHICAL PROCESSING)
================================================================================      

📖 Loading: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1.json
✅ Loaded successfully!

🔄 Starting hierarchical processing (deepest level first)...


============================================================
📊 Graph 1: 1500_CDD_TUSourcedFamilyMemberLink (Level 0)
============================================================

🔄 Processing Graph 1...
   Vertices: 1214
   🤖 Calling GPT-5 to extract files from Graph 1...
   📄 Found 4 unique file(s): ['ediHFCPropagationUserDefinedTypes.dml', 'log-info.dml', 'error-info.dml', 'Hospitals.dml']
      ⚠️  ediHFCPropagationUserDefinedTypes.dml NOT FOUND in data/ folder
      ⚠️  log-info.dml NOT FOUND in data/ folder
      ⚠️  error-info.dml NOT FOUND in data/ folder
      ⚠️  Hospitals.dml NOT FOUND in data/ folder

✅ Graph 1 processing complete!

================================================================================      
💾 Saving to: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
✅ Saved successfully! File size: 405.11 KB

================================================================================      
📊 SUMMARY
================================================================================      
Total file references embedded: 0
Output file: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
================================================================================      

✅ STEP 2 COMPLETE!

2025-11-21 11:57:00.649 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:271 - Step 2 complete: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
2025-11-21 11:57:00.649 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5... 
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
📖 Loading graph data from: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
✅ Loaded successfully!

📊 Main Graph: 1500_CDD_TUSourcedFamilyMemberLink (ID: 1)
📊 Total graphs in hierarchy: 1
📊 Subgraphs to process: 0
   Processing order: []

================================================================================      
PHASE 2: PROCESS SUBGRAPHS (BOTTOM-UP)
         Extract: detailed functional logic (inputs, transformations, outputs)        
================================================================================      

subgraph_logic_optimized.json
✅ Saved successfully! File size: 0.00 KB

================================================================================
PHASE 3: PROCESS MAIN GRAPH (HYBRID OPTIMIZATION)
         - DML reduction to required sections only
         - Identify all outputs dynamically
         - Generate mapping for each output separately
================================================================================

================================================================================
🎯 Processing Main Graph 1: 1500_CDD_TUSourcedFamilyMemberLink
================================================================================
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
📦 PHASE 0: CONTEXT-AWARE DML OPTIMIZATION
================================================================================
   ℹ️  No referenced files to optimize
   ⏳ Waiting 60 seconds to avoid rate limit...

   mponent_deeply:423 -    ✅ AI analysis: 26 steps
2025-11-21 11:56:19.523 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: LkpHospitals
2025-11-21 11:56:29.667 | DEBUG    | services.stag.abinitio_logic_extractor:_parse_component_analysis_response:454 - Failed to parse AI component analysis: Expecting ',' delimiter: line 39 column 52 (char 4842)
2025-11-21 11:56:29.667 | WARNING  | services.stag.abinitio_logic_extractor:_analyze_component_deeply:426 -    ⚠ AI analysis incomplete for LkpHospitals
2025-11-21 11:56:29.667 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: OFile_ediTUSourcedFamilyMemberLink.dat
2025-11-21 11:56:38.860 | DEBUG    | services.stag.abinitio_logic_extractor:_parse_component_analysis_response:454 - Failed to parse AI component analysis: Expecting ',' delimiter: line 38 column 47 (char 4508)
2025-11-21 11:56:38.862 | WARNING  | services.stag.abinitio_logic_extractor:_analyze_component_deeply:426 -    ⚠ AI analysis incomplete for OFile_ediTUSourcedFamilyMemberLink.dat
2025-11-21 11:56:38.862 | INFO     | services.stag.abinitio_logic_extractor:_extract_steps_from_components:333 -    ✅ Deeply analyzed 1214 components from Ab Initio graph 
2025-11-21 11:56:38.870 | INFO     | services.stag.abinitio_logic_extractor:extract_logic:124 -    🔄 Generating detailed STTM using VM_Automation...
2025-11-21 11:56:38.872 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:85 - Generating STTM for: 1500_CDD_TUSourcedFamilyMemberLink
2025-11-21 11:56:38.872 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:86 - Output folder: outputs\sttm_abinitio
2025-11-21 11:56:38.872 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:90 - Step 1: Extracting graph details...

================================================================================      
🎯 Extracting detailed hierarchy for Graph ID: 1
================================================================================      

📂 Loading subgraph JSON from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\1500_CDD_TUSourcedFamilyMemberLink_components.json
✅ Loaded 0 subgraphs from JSON
🔍 Processing graph ID: 1 (Level 0)
❌ Graph ID 1 not found in subgraphs
2025-11-21 11:56:38.925 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step1:160 - Step 1 execution failed (likely format mismatch), using enhanced extraction: Graph ID 1 not found or could not be processed
2025-11-21 11:56:38.995 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:235 - Enhanced step 1 complete with raw_content: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1.json
2025-11-21 11:56:38.995 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:236 -   Vertices: 1214, Raw content size: 10000 chars
2025-11-21 11:56:38.998 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:100 - Step 2: Embedding DML/XFR content...
🔧 Initializing GPT-5 LLM...
✅ GPT-5 LLM initialized (Model: gpt-5)

📂 Loading files from: Input Files\blade (including all subfolders)

✅ Total files loaded: 0
   Available files: []


================================================================================      
STEP 2: EMBED DML/XFR FILES (HIERARCHICAL PROCESSING)
================================================================================      

📖 Loading: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1.json
✅ Loaded successfully!

🔄 Starting hierarchical processing (deepest level first)...


============================================================
📊 Graph 1: 1500_CDD_TUSourcedFamilyMemberLink (Level 0)
============================================================

🔄 Processing Graph 1...
   Vertices: 1214
   🤖 Calling GPT-5 to extract files from Graph 1...
   📄 Found 4 unique file(s): ['ediHFCPropagationUserDefinedTypes.dml', 'log-info.dml', 'error-info.dml', 'Hospitals.dml']
      ⚠️  ediHFCPropagationUserDefinedTypes.dml NOT FOUND in data/ folder
      ⚠️  log-info.dml NOT FOUND in data/ folder
      ⚠️  error-info.dml NOT FOUND in data/ folder
      ⚠️  Hospitals.dml NOT FOUND in data/ folder

✅ Graph 1 processing complete!

================================================================================      
💾 Saving to: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
✅ Saved successfully! File size: 405.11 KB

================================================================================      
📊 SUMMARY
================================================================================      
Total file references embedded: 0
Output file: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
================================================================================      

✅ STEP 2 COMPLETE!

2025-11-21 11:57:00.649 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:271 - Step 2 complete: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
2025-11-21 11:57:00.649 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5... 
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
📖 Loading graph data from: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
✅ Loaded successfully!

📊 Main Graph: 1500_CDD_TUSourcedFamilyMemberLink (ID: 1)
📊 Total graphs in hierarchy: 1
📊 Subgraphs to process: 0
   Processing order: []

================================================================================      
PHASE 2: PROCESS SUBGRAPHS (BOTTOM-UP)
         Extract: detailed functional logic (inputs, transformations, outputs)        
================================================================================      

subgraph_logic_optimized.json
✅ Saved successfully! File size: 0.00 KB

================================================================================
PHASE 3: PROCESS MAIN GRAPH (HYBRID OPTIMIZATION)
         - DML reduction to required sections only
         - Identify all outputs dynamically
         - Generate mapping for each output separately
================================================================================      

================================================================================      
🎯 Processing Main Graph 1: 1500_CDD_TUSourcedFamilyMemberLink
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
   📝 Raw response saved to: outputs\sttm_abinitio\identify_outputs_response.txt
   📊 Response length: 245 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Identified 1 output(s):
      1. OFile_ediTUSourcedFamilyMemberLink.dat (ID: 1187)

================================================================================
📋 PHASE 2.7: GENERATE MAIN GRAPH SUMMARY
================================================================================
   🤖 Calling GPT-5 LLM to generate graph summary...
   📝 Raw response saved to: outputs\sttm_abinitio\graph_summary_response.txt
   📊 Response length: 1733 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Graph summary generated successfully
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
🔄 PHASE 3: GENERATE MAPPINGS FOR EACH OUTPUT
================================================================================

================================================================================
📋 Processing Output 1/1: OFile_ediTUSourcedFamilyMemberLink.dat (ID: 1187)
================================================================================
   📝 Prompt saved to: outputs\sttm_abinitio\output_1_prompt.txt (Length: 389,455 chars)
   🤖 Calling GPT-5 LLM for attribute mapping...
   📝 Raw response saved to: outputs\sttm_abinitio\output_1_response.txt
   📊 Response length: 3875 characters
   ✓ Attempting to parse entire response as JSON
   ⚠️  JSON parse error: Unterminated string starting at: line 55 column 25 (char 38300)
   📄 First 500 chars of response: {
  "output_name": "OFile_ediTUSourcedFamilyMemberLink.dat",
  "output_component_id": "1187",
  "output_dataset": "$AI_MFS_TEMP/${fileNamePrefix}ediTUSourcedFamilyMemberLink.dat", 
  "mappings": [
    {
      "source_dataset": "N/A (Derived from prior transformations, not directly from a single input file)",
      "source_component": "Reformat (Family Linking)",
      "source_component_id": "1148",
      "source_attribute": "hospitalfk1",
      "source_datatype": "decimal",
      "target_dataset"
   ✓ Attempting to extract JSON from position 0 to 3797
   ⚠️  Failed to extract JSON: Expecting ',' delimiter: line 53 column 6 (char 3798)  
   ⚠️  Failed to parse mapping for OFile_ediTUSourcedFamilyMemberLink.dat

================================================================================      
✅ MAIN GRAPH PROCESSING COMPLETE
   Total outputs processed: 1
================================================================================      

💾 Saving final mapping to: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_final_mapping_optimized.json
✅ Saved successfully! File size: 2.19 KB

================================================================================      
PHASE 4: GENERATE EXCEL OUTPUT
================================================================================      

📊 Generating Excel file: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_source_to_target_mapping.xlsx
   📄 Creating Summary sheet
   📄 Creating sheet: OFile_ediTUSourcedFamilyM
   ✅ Added 0 mapping entries
✅ Excel file saved! File size: 6.41 KB

================================================================================      
📋 EXECUTION SUMMARY
================================================================================      
Main Graph ID: 1
Main Graph Name: 1500_CDD_TUSourcedFamilyMemberLink
Subgraphs Processed: 0
Output Files Generated:
  - 1500_CDD_TUSourcedFamilyMemberLink_subgraph_logic_optimized.json
  - 1500_CDD_TUSourcedFamilyMemberLink_final_mapping_optimized.json
  - 1500_CDD_TUSourcedFamilyMemberLink_source_to_target_mapping.xlsx
All files saved in: outputs\sttm_abinitio
================================================================================      

✅ STEP 3 COMPLETE!

2025-11-21 12:02:34.836 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:321 - Excel file generated: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_source_to_target_mapping.xlsx
2025-11-21 12:02:34.836 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:325 - Mapping JSON generated: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_final_mapping_optimized.json
2025-11-21 12:02:34.836 | INFO     | services.stag.abinitio_logic_extractor:extract_logic:131 -    ✅ VM_Automation STTM generated: outputs\sttm_abinitio\1500_CDD_TUSourcedFamilyMemberLink_source_to_target_mapping.xlsx
2025-11-21 12:02:34.836 | INFO     | services.stag.abinitio_logic_extractor:extract_logic:152 - ✅ Extracted 1214 steps from Ab Initio graph: 1500_CDD_TUSourcedFamilyMemberLink.pset
2025-11-21 12:02:34.842 | INFO     | services.stag.stag_orchestrator:generate_comparison:169 -
📊 Step 3: Extracting Databricks logic...
2025-11-21 12:02:34.842 | INFO     | services.stag.databricks_logic_extractor:extract_logic:66 - 📊 Extracting logic for Databricks pipeline: pl_TUSourcedFamilyMemberLink  
2025-11-21 12:02:34.938 | INFO     | services.stag.databricks_logic_extractor:_search_pipeline_documents:122 -    Found 20 Databricks/ADF documents for pl_TUSourcedFamilyMemberLink
2025-11-21 12:02:34.938 | INFO     | services.stag.databricks_logic_extractor:_find_adf_json:139 -    🔍 Searching for ADF JSON: pl_TUSourcedFamilyMemberLink
2025-11-21 12:02:34.942 | INFO     | services.stag.databricks_logic_extractor:_find_adf_json:151 -    ✅ FOUND (exact match): ./Databricks_repo/adf/pipeline/pl_TUSourcedFamilyMemberLink.json
2025-11-21 12:02:34.971 | DEBUG    | services.stag.databricks_logic_extractor:_get_pipeline_parameter_default:453 -          Found parameter 'notebookpath' = '/Insleads-code/CDD/tusourcedfamilymemberlink/'
2025-11-21 12:02:34.973 | INFO     | services.stag.databricks_logic_extractor:_evaluate_adf_expression:407 -       ✅ Evaluated expression: @concat(pipeline().parameters.notebookpath,'unique_permid_patientacctid_relations') → /Insleads-code/CDD/tusourcedfamilymemberlink/unique_permid_patientacctid_relations
2025-11-21 12:02:34.975 | DEBUG    | services.stag.databricks_logic_extractor:_get_pipeline_parameter_default:453 -          Found parameter 'notebookpath' = '/Insleads-code/CDD/tusourcedfamilymemberlink/'
2025-11-21 12:02:34.975 | INFO     | services.stag.databricks_logic_extractor:_evaluate_adf_expression:407 -       ✅ Evaluated expression: @concat(pipeline().parameters.notebookpath,'unique_permid_patientacctid_relations') → /Insleads-code/CDD/tusourcedfamilymemberlink/unique_permid_patientacctid_relations
2025-11-21 12:02:35.050 | INFO     | services.stag.databricks_logic_extractor:_extract_inputs_from_notebook_code:578 -       Extracted 73 input tables from /Insleads-code/CDD/tusourcedfamilymemberlink/unique_permid_patientacctid_relations: ['C', 'globalmrnxpaccts', 'join2', 'tables', 'vw_df_delta', 'specified', 'HDFS', 'AS', 'tusourcedfamilymemberlink', 'join1', 'sql', 'functions', 'c1', 'mrnlist', 'escan', 'c2', 'ha', 'py', 'pa_permid_join', 'cdd', 'permidrelations', 'famc', 'ADLS', 'window', 'paccts', 'patientacctsaccesscoordinator', 'pac', 'various', 'hhpermid', 'hospinsurancecodes', 'patientaccts', 'other', 'permidpatientacctid_dupes', 'readwriter', 'types', 'permid', 'havalues', 'base', 'user', 'pa', 'Databricks', 'permidpatientacctid', 'to', 'c3', 'swift', 'pipeline', 'any', 'Delta', 'parquet', 'another', 'pacctpolicy', 'gmrn_swift', 'multiple', 'Patientaccts', 'ca_mcare_pac', 'patientacctxpermid', 'pacctpolicyid', 'the', 'functools', 'both', 'PermID', 'vsnap', 'edipayers', 'patient', 'delta', 'CDD', 'pyspark', 'paths', 'hhview', 'configparser', 'patientacctscodes', 'ca_mcare', 'a']
2025-11-21 12:02:35.054 | DEBUG    | services.stag.databricks_logic_extractor:_get_pipeline_parameter_default:453 -          Found parameter 'notebookpath' = '/Insleads-code/CDD/tusourcedfamilymemberlink/'
2025-11-21 12:02:35.054 | INFO     | services.stag.databricks_logic_extractor:_evaluate_adf_expression:407 -       ✅ Evaluated expression: @concat(pipeline().parameters.notebookpath,'unique_permid_patientacctid_relations') → /Insleads-code/CDD/tusourcedfamilymemberlink/unique_permid_patientacctid_relations
2025-11-21 12:02:35.174 | INFO     | services.stag.databricks_logic_extractor:_extract_inputs_from_notebook_code:578 -       Extracted 46 input tables from /Insleads-code/Common-Util/update_notification: ['Cosmos', 'specified', 'sql', 'notification', 'policyinfotable', 'or', 'functions', 'escincpeople', 'distinct_src_load3', 'operations_log_360', 'joined', 'policy', 'CosmosDB', 'json', 'all_leads', 'py', 'externallysourcedsubscriberdob', 'window', 'markedgoodrows', 'types', 'widgets', 'policyinfo1', 'user', 'source', 'Databricks', 'pipeline', 'ICG', 'datetime', 'LSB', 'Staging', 'jdbc', 'Azure', 'MapR', 'table', 'externallysourcedsubscriberdob_staging', 'the', 'functools', 'staging', 'deduped_subdob_verified', 'an', 'globalcosmosdb', 'paths', 'pyspark', 'corruptrowstable', 'csv', 'a']
2025-11-21 12:02:35.248 | INFO     | services.stag.databricks_logic_extractor:_extract_inputs_from_notebook_code:578 -       Extracted 49 input tables from /Insleads-code/Common-Util/360_logger_v1: ['databases', 'specified', 'sql', 'eith



