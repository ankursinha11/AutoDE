D-14.dml', 'lkpPatientAcctsXRefPermID-31.dml', 'lkpPatientAcctsXRefPermIDJoin-15.dml', 'lkpPatientAcctsXRefPermIDJoin-32.dml', 'lkpPayerPrefixByState-19.dml', 'lkpStateCountByPayer-20.dml', 'lkpVSnapGlobalMRNxPacct-16.dml', 'lkpVSnapGlobalMRNxPacct-33.dml', 'lkpediGenEDIDataSources-21.dml', 'mCareUserDefinedFunctions.xfr', 'mCareUserDefinedTypes.dml', 'patbg.dml', 'vSnapGlobalMRNFamilyHelperAccounts.dml', 'vSnapGlobalMRNFamilyHelperAccounts_Deleted.dml', 'vSnapGlobalMRNxHospInsuranceCodes.dml', 'vSnapGlobalMRNxHospInsuranceCodes_Deleted.dml', 'vSnapGlobalMRNxPacct.dml', 'vSnapGlobalMRNxPacct_Deleted.dml', 'vwEDIAbiLeadSourceTransitionToICD.dml', 'vwEDIAbiLeadSourceTransitionToICD_Deleted.dml', 'vwEDILeadSourceTransition.dml']


================================================================================      
STEP 2: EMBED DML/XFR FILES (HIERARCHICAL PROCESSING)
================================================================================      

📖 Loading: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1.json      
✅ Loaded successfully!

🔄 Starting hierarchical processing (deepest level first)...


============================================================
📊 Graph 1: 1500_CDD_TUSourcedFamilyMemberLink (Level 0)
============================================================

🔄 Processing Graph 1...
   Vertices: 584
   🤖 Calling GPT-5 to extract files from Graph 1...
   📄 Found 4 unique file(s): ['log-info.dml', 'Hospitals.dml', 'ediHFCPropagationUserDefinedTypes.dml', 'error-info.dml']
      ⚠️  log-info.dml NOT FOUND in data/ folder
      ✅ Hospitals.dml (content embedded)
      ✅ ediHFCPropagationUserDefinedTypes.dml (content embedded)
      ⚠️  error-info.dml NOT FOUND in data/ folder
   ✅ Embedded 2 file(s) at graph level

✅ Graph 1 processing complete!

================================================================================      
💾 Saving to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
✅ Saved successfully! File size: 277.40 KB

================================================================================      
📊 SUMMARY
================================================================================      
Total file references embedded: 2
Output file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
================================================================================      

✅ STEP 2 COMPLETE!

2025-11-21 21:15:28.174 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:271 - Step 2 complete: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
2025-11-21 21:15:28.174 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5... 
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
📖 Loading graph data from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_detailed_graph1_with_files.json
✅ Loaded successfully!

📊 Main Graph: 1500_CDD_TUSourcedFamilyMemberLink (ID: 1)
📊 Total graphs in hierarchy: 1
📊 Subgraphs to process: 0
   Processing order: []

         Extract: detailed functional logic (inputs, transformations, outputs)
================================================================================

💾 Saving subgraph logic to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_subgraph_logic_optimized.json
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
   📊 Original referenced files: 2 files
   📏 Original DML content size: 69,243 characters
   🤖 Asking LLM to identify required DML sections...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\dml_optimization_response.txt
   📊 Response length: 3553 characters
   ✓ Attempting to parse entire response as JSON
   ⚠️  JSON parse error: Unterminated string starting at: line 72 column 11 (char 3543)
   📄 First 500 chars of response: {
  "required_sections": {
    "ediHFCPropagationUserDefinedTypes.dml": [
      {
        "type_name": "typeTUSourcedFamilyMemberLinkStaging",
        "required_fields": [
          "decimal(\"\\x01\") hospitalfk1",
          "decimal(\"\\x01\") patientacctifk1",
          "decimal(\"\\x01\") hospitalfk2",
          "decimal(\"\\x01\") patientacctifk2",
          "string(1) newline = \"\\n\""
        ]
      },
      {
        "type_name": "typePatientAcctsXRefPermID",
        "required_fields":
   ✓ Attempting to extract JSON from position 0 to 2706
   ⚠️  Failed to extract JSON: Expecting ',' delimiter: line 56 column 8 (char 2707)
   ⚠️  Failed to identify required sections, using full DML content
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
🔍 PHASE 2.5: IDENTIFY OUTPUT COMPONENTS
================================================================================
   🤖 Asking LLM to identify all outputs...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\identify_outputs_response.txt
   📊 Response length: 395 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Identified 2 output(s):
      1. Output_File (ID: 567)
      2. OFile_ediTUSourcedFamilyMemberLink.dat (ID: ds_4)

================================================================================
📋 PHASE 2.7: GENERATE MAIN GRAPH SUMMARY
================================================================================
   🤖 Calling GPT-5 LLM to generate graph summary...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\graph_summary_response.txt
   📊 Response length: 1999 characters
   ✓ Attempting to parse entire response as JSON
   ✅ Graph summary generated successfully
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
🔄 PHASE 3: GENERATE MAPPINGS FOR EACH OUTPUT
================================================================================

================================================================================
📋 Processing Output 1/2: Output_File (ID: 567)
================================================================================
   📝 Prompt saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_1_prompt.txt (Length: 273,606 chars)
   🤖 Calling GPT-5 LLM for attribute mapping...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_1_response.txt
   📊 Response length: 4162 characters
   ✓ Attempting to parse entire response as JSON
   ⚠️  JSON parse error: Unterminated string starting at: line 75 column 27 (char 4161)
   📄 First 500 chars of response: {
  "output_name": "Output_File",
  "output_component_id": "567",
  "output_dataset": "$AI_MFS_TEMP/${fileNamePrefix}ediTUSourcedFamilyMemberLink.dat",
  "mappings": [
    {
      "source_dataset": "Unknown (not enough information to identify specific input file)",
      "source_component": "Unknown",
      "source_component_id": "Unknown",
      "source_attribute": "hospitalfk1",
      "source_datatype": "decimal",
      "target_dataset": "$AI_MFS_TEMP/${fileNamePrefix}ediTUSourcedFamilyMemberL
   ✓ Attempting to extract JSON from position 0 to 4022
   ⚠️  Failed to extract JSON: Unterminated string starting at: line 72 column 25 (char 3992)
   ⚠️  Failed to parse mapping for Output_File
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
📋 Processing Output 2/2: OFile_ediTUSourcedFamilyMemberLink.dat (ID: ds_4)
================================================================================
   📝 Prompt saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_2_prompt.txt (Length: 273,702 chars)
   🤖 Calling GPT-5 LLM for attribute mapping...
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\output_2_response.txt
   📊 Response length: 4455 characters
   ✓ Attempting to parse entire response as JSON
   ⚠️  JSON parse error: Unterminated string starting at: line 63 column 18 (char 4026)
   📄 First 500 chars of response: {
  "output_name": "OFile_ediTUSourcedFamilyMemberLink.dat",
  "output_component_id": "ds_4",
  "output_dataset": "OFile_ediTUSourcedFamilyMemberLink.dat",
  "mappings": [
    {
      "source_dataset": "Unknown (not enough information in provided context to identify exact input file/component)",
      "source_component": "Unknown",
      "source_component_id": "Unknown",
      "source_attribute": "hospitalfk1",
      "source_datatype": "decimal",
      "target_dataset": "OFile_ediTUSourcedFamily
   ✓ Attempting to extract JSON from position 0 to 3439
   ⚠️  Failed to extract JSON: Expecting ',' delimiter: line 50 column 6 (char 3440)
   ⚠️  Failed to parse mapping for OFile_ediTUSourcedFamilyMemberLink.dat

================================================================================
✅ MAIN GRAPH PROCESSING COMPLETE
   Total outputs processed: 2
================================================================================

💾 Saving final mapping to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_final_mapping_optimized.json
✅ Saved successfully! File size: 2.69 KB

================================================================================
PHASE 4: GENERATE EXCEL OUTPUT
================================================================================

📊 Generating Excel file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_source_to_target_mapping.xlsx
   📄 Creating Summary sheet
   📄 Creating sheet: Output_1_Output_File
   ✅ Added 0 mapping entries
   📄 Creating sheet: Output_2_OFile_ediTUSourcedFa
   ✅ Added 0 mapping entries
✅ Excel file saved! File size: 7.24 KB

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
All files saved in: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation
================================================================================      

✅ STEP 3 COMPLETE!

2025-11-21 21:21:06.663 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:321 - Excel file generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_source_to_target_mapping.xlsx
2025-11-21 21:21:06.664 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step3:325 - Mapping JSON generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\1500_CDD_TUSourcedFamilyMemberLink_final_mapping_optimized.json
2025-11-21 21:21:06.668 | INFO     | services.local_search.local_search_client:index_documents:105 - Indexing 1 documents...
2025-11-21 21:21:06.668 | INFO     | services.local_search.local_search_client:index_documents:155 - Generating embeddings...
Batches: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00,  8.33it/s]
2025-11-21 21:21:07.263 | INFO     | services.local_search.local_search_client:index_documents:262 - ✓ Indexed 1 documents successfully (upserted)
