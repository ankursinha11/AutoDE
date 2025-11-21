Project root not found: Input Files\blade
2025-11-21 19:53:49.256 | INFO     | parsers.abinitio.vm_fawn_auto_runner:__init__:51 - VM_FAWN Auto Runner initialized
2025-11-21 19:53:49.257 | INFO     | parsers.abinitio.vm_fawn_auto_runner:__init__:52 -   VM_FAWN directory: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\vm_fawn
2025-11-21 19:53:49.258 | INFO     | parsers.abinitio.vm_fawn_auto_runner:__init__:53 -   Output directory: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\Input Files\VM_FAWN\bbi_preprocessing_output
2025-11-21 19:53:49.260 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:71 - Running VM_FAWN on: 1300_CDD_PatientAcctsXRefPermID.mp
2025-11-21 19:53:50.733 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:113 -   Loading VM_FAWN patterns...
2025-11-21 19:53:50.763 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:116 -   Converting MP to TXT...
2025-11-21 19:53:53.281 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:128 -   Parsing with VM_FAWN...
2025-11-21 19:53:56.075 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:138 -   Processing and generating Excel...
2025-11-21 19:53:57.494 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:178 - ✅ VM_FAWN Excel generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\Input Files\VM_FAWN\bbi_preprocessing_output\1300_CDD_PatientAcctsXRefPermID_auto.xlsx
🔄 Converting VM_FAWN Excel to Enhanced JSON
   Input Excel: 1300_CDD_PatientAcctsXRefPermID_auto.xlsx
   Source MP: 1300_CDD_PatientAcctsXRefPermID.mp

📊 Step 1: Applying intelligent filtering to reduce noise...
   ✅ Filtered: 1655 → 805 rows (51.4% reduction)

📖 Step 2: Reading Excel sheets...
   Sheets found: ['DataSet', 'Component&Fields', 'GraphParameters', 'GraphFlow']
   ✓ DataSet: 12 rows
   ✓ Component&Fields: 805 rows
   ✓ GraphParameters: 1731 rows
   ✓ GraphFlow: 0 rows
   ⚠️ GraphFlow sheet empty, extracting flows from .mp binary format...
   ✓ Extracted 1145 flows from .mp file
   ✓ Generated 2 ports from flows

✅ Conversion complete:
   Vertices: 817 (Transforms: 805, Datasets: 0)
   Flows: 1145
   Graphs: 1
🔍 ENHANCED PARSING - EXTRACTING ALL COMPONENTS
============================================================
   📊 Extracting graphs with fixed name parsing...
      • Found 200 graphs with corrected names

📊 FLOW TYPE ANALYSIS:
   Total flows: 1145
   Flow connections: 2290
   📋 FLOW TYPE DISTRIBUTION:
      • PARAMETER_FLOW: 1145
   📋 CONFIG FLOWS IDENTIFIED: 1145

✅ ENHANCED PARSING COMPLETE:
   📊 COMPONENT SUMMARY:
      • Total Graphs: 200
      • Total Vertices: 1740
      • Total Flows: 1145
      • Total Ports: 8190
      • Total Flow Connections: 2290
      • Total Port Bindings: 972
      • Total Config Flows: 1145
      • Total Graph Vertex Links: 1459
      • Total Graph Flow Links: 1145

💾 Enhanced components saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\1300_CDD_PatientAcctsXRefPermID_enhanced_temp.json
Diagram generation failed: failed to execute WindowsPath('dot'), make sure the Graphviz executables are on your systems' PATH
Traceback (most recent call last):
  File "C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\venv\Lib\site-packages\graphviz\backend\execute.py", line 78, in run_check
    proc = subprocess.run(cmd, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Ankur.Sinha\Downloads\Python312\Python312\Lib\subprocess.py", line 548, in run
    with Popen(*popenargs, **kwargs) as process:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Ankur.Sinha\Downloads\Python312\Python312\Lib\subprocess.py", line 1026, in __init__
    self._execute_child(args, executable, preexec_fn, close_fds,
  File "C:\Users\Ankur.Sinha\Downloads\Python312\Python312\Lib\subprocess.py", line 1538, in _execute_child
    hp, ht, pid, tid = _winapi.CreateProcess(executable, args,
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [WinError 2] The system cannot find the file specified

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\graph_flow\diagram_generator.py", line 186, in generate_diagram
    diagram_path = dot.render(
                   ^^^^^^^^^^^
  File "C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\venv\Lib\site-packages\graphviz\_tools.py", line 185, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\venv\Lib\site-packages\graphviz\rendering.py", line 122, in render
    rendered = self._render(*args, **kwargs)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\venv\Lib\site-packages\graphviz\_tools.py", line 185, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\venv\Lib\site-packages\graphviz\backend\rendering.py", line 326, in render
    execute.run_check(cmd,
  File "C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\venv\Lib\site-packages\graphviz\backend\execute.py", line 81, in run_check
    raise ExecutableNotFound(cmd) from e
graphviz.backend.execute.ExecutableNotFound: failed to execute WindowsPath('dot'), make sure the Graphviz executables are on your systems' PATH
2025-11-21 19:54:08.871 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:85 - Generating STTM for: 1300_CDD_PatientAcctsXRefPermID
2025-11-21 19:54:08.878 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:86 - Output folder: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation
2025-11-21 19:54:08.881 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:90 - Step 1: Extracting graph details...

================================================================================      
🎯 Extracting detailed hierarchy for Graph ID: 1
================================================================================      

📂 Loading subgraph JSON from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\1300_CDD_PatientAcctsXRefPermID_components.json
✅ Loaded 0 subgraphs from JSON
🔍 Processing graph ID: 1 (Level 0)
❌ Graph ID 1 not found in subgraphs
2025-11-21 19:54:08.934 | WARNING  | parsers.abinitio.automation.abinitio_sttm_generator:_run_step1:160 - Step 1 execution failed (likely format mismatch), using enhanced extraction: Graph ID 1 not found or could not be processed
2025-11-21 19:54:08.999 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:235 - Enhanced step 1 complete with raw_content: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_detailed_graph1.json
2025-11-21 19:54:09.000 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_simplified_step1:236 -   Vertices: 817, Raw content size: 10000 chars
2025-11-21 19:54:09.006 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:100 - Step 2: Embedding DML/XFR content...
🔧 Initializing GPT-5 LLM...
✅ GPT-5 LLM initialized (Model: gpt-5)

📂 Loading files from: Input Files\blade\dml (including all subfolders)

✅ Total files loaded: 0
   Available files: []


================================================================================      
STEP 2: EMBED DML/XFR FILES (HIERARCHICAL PROCESSING)
================================================================================      

📖 Loading: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_detailed_graph1.json
✅ Loaded successfully!

🔄 Starting hierarchical processing (deepest level first)...


============================================================
📊 Graph 1: 1300_CDD_PatientAcctsXRefPermID (Level 0)
============================================================

🔄 Processing Graph 1...
   Vertices: 817
   🤖 Calling GPT-5 to extract files from Graph 1...
   📄 Found 9 unique file(s): ['ediHFCPropagationUserDefinedTypes.dml', 'log-info.dml', 'EDIPayers_TUPayerIDMapping.dml', 'HospitalPayerCOB.dml', 'TU_Policy_InsuranceEligibility_Actions.dml', 'FilterPayerGroupNumForOHI.dml', 'TU_eScan_ExcludeIEClientID.dml', 'error-info.dml', 'Hospitals.dml']
      ⚠️  ediHFCPropagationUserDefinedTypes.dml NOT FOUND in data/ folder
      ⚠️  log-info.dml NOT FOUND in data/ folder
      ⚠️  EDIPayers_TUPayerIDMapping.dml NOT FOUND in data/ folder
      ⚠️  HospitalPayerCOB.dml NOT FOUND in data/ folder
      ⚠️  TU_Policy_InsuranceEligibility_Actions.dml NOT FOUND in data/ folder        
      ⚠️  FilterPayerGroupNumForOHI.dml NOT FOUND in data/ folder
      ⚠️  TU_eScan_ExcludeIEClientID.dml NOT FOUND in data/ folder
      ⚠️  error-info.dml NOT FOUND in data/ folder
      ⚠️  Hospitals.dml NOT FOUND in data/ folder

✅ Graph 1 processing complete!

================================================================================      
💾 Saving to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_detailed_graph1_with_files.json
✅ Saved successfully! File size: 281.45 KB

================================================================================      
📊 SUMMARY
================================================================================      
Total file references embedded: 0
Output file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_detailed_graph1_with_files.json
================================================================================      

✅ STEP 2 COMPLETE!

2025-11-21 19:54:32.829 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:271 - Step 2 complete: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_detailed_graph1_with_files.json
2025-11-21 19:54:32.829 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5... 
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
📖 Loading graph data from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_detailed_graph1_with_files.json
✅ Loaded successfully!

📊 Main Graph: 1300_CDD_PatientAcctsXRefPermID (ID: 1)
📊 Total graphs in hierarchy: 1
📊 Subgraphs to process: 0
   Processing order: []

================================================================================      
PHASE 2: PROCESS SUBGRAPHS (BOTTOM-UP)
         Extract: detailed functional logic (inputs, transformations, outputs)        
================================================================================      

💾 Saving subgraph logic to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_subgraph_logic_optimized.json
✅ Saved successfully! File size: 0.00 KB

================================================================================      
PHASE 3: PROCESS MAIN GRAPH (HYBRID OPTIMIZATION)
         - DML reduction to required sections only
         - Identify all outputs dynamically
         - Generate mapping for each output separately
================================================================================      

================================================================================      
🎯 Processing Main Graph 1: 1300_CDD_PatientAcctsXRefPermID
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
   📊 Response length: 445 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Identified 2 output(s):
      1. OFile_ediPatientAcctsXRefPermID.dat (ID: 772)
      2. OFile_ediPatientAcctsXRefPermIDUpdate.dat (ID: 774)

================================================================================      
📋 PHASE 2.7: GENERATE MAIN GRAPH SUMMARY
================================================================================      
   🤖 Calling GPT-5 LLM to generate graph summary...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\graph_summary_response.txt
   📊 Response length: 3035 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Graph summary generated successfully
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================      
🔄 PHASE 3: GENERATE MAPPINGS FOR EACH OUTPUT
================================================================================      

================================================================================      
📋 Processing Output 1/2: OFile_ediPatientAcctsXRefPermID.dat (ID: 772)
================================================================================      
   📝 Prompt saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_1_prompt.txt (Length: 271,404 chars)       
   🤖 Calling GPT-5 LLM for attribute mapping...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_1_response.txt
   📊 Response length: 3694 characters
   ✓ Attempting to parse entire response as JSON
   ⚠️  JSON parse error: Unterminated string starting at: line 64 column 27 (char 36933)
   📄 First 500 chars of response: {
  "output_name": "OFile_ediPatientAcctsXRefPermID.dat",
  "output_component_id": "772",
  "output_dataset": "$AI_MFS_TEMP/${fileNamePrefix}ediPatientAcctsXRefPermID.dat",    
  "mappings": [
    {
      "source_dataset": "IFilePatientAcctsXRefPermID",
      "source_component": "IFilePatientAcctsXRefPermID",
      "source_component_id": "775",
      "source_attribute": "hospitalfk",
      "source_datatype": "decimal",
      "target_dataset": "$AI_MFS_TEMP/${fileNamePrefix}ediPatientAcctsXRefPermID.     
   ✓ Attempting to extract JSON from position 0 to 3510
   ⚠️  Failed to extract JSON: Expecting ',' delimiter: line 59 column 6 (char 3511)  
   ⚠️  Failed to parse mapping for OFile_ediPatientAcctsXRefPermID.dat
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================      
📋 Processing Output 2/2: OFile_ediPatientAcctsXRefPermIDUpdate.dat (ID: 774)
================================================================================      
   📝 Prompt saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_2_prompt.txt (Length: 271,458 chars)       
   🤖 Calling GPT-5 LLM for attribute mapping...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_2_response.txt
   📊 Response length: 203 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Generated 0 attribute mapping(s) for OFile_ediPatientAcctsXRefPermIDUpdate.dat  

================================================================================      
✅ MAIN GRAPH PROCESSING COMPLETE
   Total outputs processed: 2
================================================================================      

💾 Saving final mapping to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_final_mapping_optimized.json
✅ Saved successfully! File size: 3.77 KB

================================================================================      
PHASE 4: GENERATE EXCEL OUTPUT
================================================================================      

📊 Generating Excel file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_source_to_target_mapping.xlsx
   📄 Creating Summary sheet
   📄 Creating sheet: Output_1_OFile_ediPatientAcct
seIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_source_to_target_mapping.xlsx
   📄 Creating Summary sheet
   📄 Creating sheet: Output_1_OFile_ediPatientAcct
   ✅ Added 0 mapping entries
   📄 Creating Summary sheet
   📄 Creating sheet: Output_1_OFile_ediPatientAcct
   📄 Creating sheet: Output_1_OFile_ediPatientAcct
   ✅ Added 0 mapping entries
   📄 Creating sheet: Output_2_OFile_ediPatientAcct
   ✅ Added 0 mapping entries
✅ Excel file saved! File size: 7.31 KB

================================================================================      
📋 EXECUTION SUMMARY
================================================================================      
Main Graph ID: 1
Main Graph Name: 1300_CDD_PatientAcctsXRefPermID
Subgraphs Processed: 0
Output Files Generated:
  - 1300_CDD_PatientAcctsXRefPermID_subgraph_logic_optimized.json
  - 1300_CDD_PatientAcctsXRefPermID_final_mapping_optimized.json
  - 1300_CDD_PatientAcctsXRefPermID_source_to_target_mapping.xlsx
All files saved in: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation
================================================================================      

✅ STEP 3 COMPLETE!

2025-11-21 19:59:54.600 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:321 - Excel file generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_source_to_target_mapping.xlsx
2025-11-21 19:59:54.600 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:325 - Mapping JSON generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1300_CDD_PatientAcctsXRefPermID_final_mapping_optimized.json
2025-11-21 19:59:54.605 | INFO     | services.local_search.local_search_client:index_documents:105 - Indexing 1 documents...
2025-11-21 19:59:54.605 | INFO     | services.local_search.local_search_client:index_documents:155 - Generating embeddings...
Batches: 100%|█████████████████████████████████████████| 1/1 [00:00<00:00,  4.93it/s] 
2025-11-21 19:59:56.813 | INFO     | services.local_search.local_search_client:index_documents:262 - ✓ Indexed 1 documents successfully (upserted)
