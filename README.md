================================================================================
STEP 2: EMBED DML/XFR FILES (HIERARCHICAL PROCESSING)
================================================================================

📖 Loading: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_detailed_graph1.json
✅ Loaded successfully!

🔄 Starting hierarchical processing (deepest level first)...       


============================================================       
📊 Graph 1: 265_fileTransferToHadoopServerAdhoc_remove (Level 0)   
============================================================       

🔄 Processing Graph 1...
   Vertices: 427
   🤖 Calling GPT-5 to extract files from Graph 1...
❌ Error invoking GPT-5: Error code: 429 - {'error': {'code': 'RateLimitReached', 'message': 'Your requests to gpt-5 for gpt-5 in East US 2 have exceeded the token rate limit for your current AIServices S0 pricing tier. This request was for ChatCompletions_Create under Azure OpenAI API version 2024-12-01-preview. Please retry after 60 seconds. To increase your default rate limit, visit: https://aka.ms/oai/quotaincrease.'}}
   ❌ Error extracting files: Error code: 429 - {'error': {'code': 'RateLimitReached', 'message': 'Your requests to gpt-5 for gpt-5 in East US 2 have exceeded the token rate limit for your current AIServices S0 pricing tier. This request was for ChatCompletions_Create under Azure OpenAI API version 2024-12-01-preview. Please retry after 60 seconds. To increase your default rate limit, visit: https://aka.ms/oai/quotaincrease.'}}
   ➖ No .dml/.xfr files referenced

✅ Graph 1 processing complete!

================================================================================
💾 Saving to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_detailed_graph1_with_files.json
✅ Saved successfully! File size: 2330.66 KB

================================================================================
📊 SUMMARY
================================================================================
Total file references embedded: 0
Output file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_detailed_graph1_with_files.json
================================================================================

✅ STEP 2 COMPLETE!

2025-11-19 18:51:43.243 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:_run_step2:271 - Step 2 complete: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_detailed_graph1_with_files.json
2025-11-19 18:51:43.243 | INFO     | parsers.abinitio.automation.abinitio_sttm_generator:generate_sttm_from_parsed_json:110 - Step 3: Generating STTM mapping with GPT-5...
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
📖 Loading graph data from: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_detailed_graph1_with_files.json
✅ Loaded successfully!

📊 Main Graph: 265_fileTransferToHadoopServerAdhoc_remove (ID: 1)  
📊 Total graphs in hierarchy: 1
📊 Subgraphs to process: 0
   Processing order: []

================================================================================
PHASE 2: PROCESS SUBGRAPHS (BOTTOM-UP)
         Extract: detailed functional logic (inputs, transformations, outputs)
================================================================================

💾 Saving subgraph logic to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_subgraph_logic_optimized.json 
✅ Saved successfully! File size: 0.00 KB

================================================================================
PHASE 3: PROCESS MAIN GRAPH (HYBRID OPTIMIZATION)
         - DML reduction to required sections only
         - Identify all outputs dynamically
         - Generate mapping for each output separately
================================================================================

================================================================================
🎯 Processing Main Graph 1: 265_fileTransferToHadoopServerAdhoc_remove
================================================================================
   ⏳ Waiting 60 seconds to avoid rate limit...
  Stopping...

================================================================================
📦 PHASE 0: CONTEXT-AWARE DML OPTIMIZATION
================================================================================
   ℹ️  No referenced files to optimize
   ⏳ Waiting 60 seconds to avoid rate limit...

================================================================================
🔍 PHASE 2.5: IDENTIFY OUTPUT COMPONENTS
================================================================================
   🤖 Asking LLM to identify all outputs...
2025-11-19 18:55:43.822 | ERROR    | services.ai_script_analyzer:_create_chat_completion:80 - Error in _create_chat_completion: Error code: 429 - {'error': {'code': 'RateLimitReached', 'message': 'Your requests to gpt-4 for gpt-4.1 in South Central US have exceeded the token rate limit for your current OpenAI S0 pricing tier. This request was for ChatCompletions_Create under Azure OpenAI API version 2024-02-15-preview. Please retry after 60 seconds. To increase your default rate limit, visit: https://aka.ms/oai/quotaincrease.'}}  
2025-11-19 18:55:43.822 | ERROR    | services.ai_script_analyzer:analyze_with_context:610 - Error in analyze_with_context: Error code: 429 - {'error': {'code': 'RateLimitReached', 'message': 'Your requests to gpt-4 for gpt-4.1 in South Central US have exceeded the token rate limit for your current OpenAI S0 pricing tier. This request was for ChatCompletions_Create under Azure OpenAI API version 2024-02-15-preview. Please retry after 60 seconds. To increase your default rate limit, visit: https://aka.ms/oai/quotaincrease.'}}       
   📝 Raw response saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\identify_outputs_response.txt
   📊 Response length: 490 characters
   ✓ Attempting to parse entire response as JSON
   ⚠️  JSON parse error: Expecting value: line 1 column 1 (char 0) 
   📄 First 500 chars of response: I found some relevant information but couldn't generate a detailed analysis. Error: Error code: 429 - {'error': {'code': 'RateLimitReached', 'message': 'Your requests to gpt-4 for gpt-4.1 in South Central US have exceeded the token rate limit for your current OpenAI S0 pricing tier. This request was for ChatCompletions_Create under Azure OpenAI API version 2024-02-15-preview. Please retry after 60 seconds. To increase your default rate limit, visit: https://aka.ms/oai/quotaincrease.'}}
   ✓ Attempting to extract JSON from position 102 to 489
   ⚠️  Failed to extract JSON: Expecting property name enclosed in  double quotes: line 1 column 2 (char 1)
   ⚠️  Failed to identify outputs from LLM response
   ⚠️  No outputs identified, cannot generate mappings

💾 Saving final mapping to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_final_mapping_optimized.json   
✅ Saved successfully! File size: 0.15 KB

================================================================================
PHASE 4: GENERATE EXCEL OUTPUT
================================================================================

📊 Generating Excel file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\sttm_automation\265_fileTransferToHadoopServerAdhoc_remove_source_to_target_mapping.xlsx    
   📄 Creating Summary sheet
   ⚠️  No outputs found in mapping
✅ Excel file saved! File size: 5.50 KB

================================================================================
📋 EXECUTION SUMMARY
================================================================================
Main Graph ID: 1
Main Graph Name: 265_fileTransferToHadoopServerAdhoc_remove      
