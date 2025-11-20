For `use_container_width=True`, use `width='stretch'`. For `use_container_width=False`, use `width='content'`.
2025-11-20 20:14:26.949 | INFO     | parsers.abinitio.vm_fawn_auto_runner:__init__:51 - VM_FAWN Auto Runner initialized
2025-11-20 20:14:26.950 | INFO     | parsers.abinitio.vm_fawn_auto_runner:__init__:52 -   VM_FAWN directory: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\parsers\abinitio\vm_fawn
2025-11-20 20:14:26.950 | INFO     | parsers.abinitio.vm_fawn_auto_runner:__init__:53 -   Output directory: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\Input Files\VM_FAWN\bbi_preprocessing_output
2025-11-20 20:14:26.951 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:71 - Running VM_FAWN on: 1500_CDD_TUSourcedFamilyMemberLink.mp
2025-11-20 20:14:26.960 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:98 -   Loading VM_FAWN patterns...
2025-11-20 20:14:26.977 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:101 -   Converting MP to TXT...
2025-11-20 20:14:28.858 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:113 -   Parsing with VM_FAWN...
2025-11-20 20:14:30.958 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:123 -   Processing and generating Excel...      
2025-11-20 20:14:31.942 | INFO     | parsers.abinitio.vm_fawn_auto_runner:run_vm_fawn:163 - ✅ VM_FAWN Excel generated: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\Input Files\VM_FAWN\bbi_preprocessing_output\1500_CDD_TUSourcedFamilyMemberLink_auto.xlsx
🔄 Converting VM_FAWN Excel to Enhanced JSON
   Input Excel: 1500_CDD_TUSourcedFamilyMemberLink_auto.xlsx       
   Source MP: 1500_CDD_TUSourcedFamilyMemberLink.mp
   Sheets found: ['DataSet', 'Component&Fields', 'GraphParameters', 'GraphFlow']
   ✓ DataSet: 5 rows
   ✓ Component&Fields: 1209 rows
   ✓ GraphParameters: 1297 rows
   ✓ GraphFlow: 0 rows
   ⚠️ GraphFlow sheet empty, extracting flows from .mp binary formaat...
   ✓ Extracted 855 flows from .mp file
   ✓ Generated 2 ports from flows

✅ Conversion complete:
   Vertices: 1214 (Transforms: 1209, Datasets: 0)
   Flows: 855
   Graphs: 1
   ✅ Saved to: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\1500_CDD_TUSourcedFamilyMemberLink_components.json
2025-11-20 20:14:32.657 | INFO     | services.local_search.local_search_client:index_documents:105 - Indexing 1 documents...
2025-11-20 20:14:32.657 | INFO     | services.local_search.local_search_client:index_documents:155 - Generating embeddings...
Batches: 100%|██████████████████████| 1/1 [00:00<00:00,  4.73it/s] 
2025-11-20 20:14:33.170 | INFO     | services.local_search.local_search_client:index_documents:262 - ✓ Indexed 1 documents successfully (upserted)
2025-11-20 20:17:23.140 | INFO     | services.stag.stag_orchestrator:generate_comparison:92 - ================================================================================
2025-11-20 20:17:23.140 | INFO     | services.stag.stag_orchestrator:generate_comparison:93 - 🎯 STAG COMPARISON: ABINITIO → Databricks
2025-11-20 20:17:23.140 | INFO     | services.stag.stag_orchestrator:generate_comparison:94 - Source Workflow: 1500_CDD_TUSourcedFamilyMemberLink.pset
2025-11-20 20:17:23.140 | INFO     | services.stag.stag_orchestrator:generate_comparison:95 - ================================================================================
2025-11-20 20:17:23.140 | INFO     | services.stag.stag_orchestrator:generate_comparison:111 -
📋 Step 1: Looking up Databricks mapping...
2025-11-20 20:17:23.140 | INFO     | services.stag.system_mapping_service:get_databricks_mapping:86 - ✅ Exact match found: '1500_CDD_TUSourcedFamilyMemberLink.pset' = '1500_CDD_TUSourcedFamilyMemberLink.pset'
2025-11-20 20:17:23.140 | INFO     | services.stag.stag_orchestrator:generate_comparison:147 -    ✅ Found mapping: 1500_CDD_TUSourcedFamilyMemberLink.pset → pl_TUSourcedFamilyMemberLink
2025-11-20 20:17:23.140 | INFO     | services.stag.stag_orchestrator:generate_comparison:154 -
📊 Step 2: Extracting abinitio logic...
2025-11-20 20:17:23.140 | INFO     | services.stag.abinitio_logic_extractor:extract_logic:93 - 📊 Extracting logic for Ab Initio graph: 1500_CDD_TUSourcedFamilyMemberLink.pset
2025-11-20 20:17:23.428 | INFO     | services.stag.abinitio_logic_extractor:_search_graph_documents:253 -    Found 10 Ab Initio documents for 1500_CDD_TUSourcedFamilyMemberLink.pset
2025-11-20 20:17:23.429 | INFO     | services.stag.abinitio_logic_extractor:_get_parsed_json_path:271 -    Found parsed JSON: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\outputs\parsed_abinitio\1500_CDD_TUSourcedFamilyMemberLink_components.json
2025-11-20 20:17:23.447 | INFO     | services.stag.abinitio_logic_extractor:_load_parsed_components:285 -    ✅ Loaded parsed components: 1214 vertices
2025-11-20 20:17:23.447 | INFO     | services.stag.abinitio_logic_extractor:_extract_steps_from_components:297 -    Analyzing 1214 vertices with DEEP AI component analysis...
2025-11-20 20:17:23.448 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: AES_Encrypt
2025-11-20 20:17:33.646 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 20 steps
2025-11-20 20:17:33.647 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: AES_Encrypt
2025-11-20 20:17:43.003 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 25 steps
2025-11-20 20:17:43.003 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Trash
2025-11-20 20:17:49.408 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 27 steps
2025-11-20 20:17:49.408 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Merge
2025-11-20 20:18:01.528 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 26 steps
2025-11-20 20:18:01.528 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Merge
2025-11-20 20:18:08.859 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 26 steps
2025-11-20 20:18:08.859 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Replicate
2025-11-20 20:18:17.252 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 30 steps
2025-11-20 20:18:17.253 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Run_Program
2025-11-20 20:18:28.726 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 28 steps
2025-11-20 20:18:28.726 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Run_Program
2025-11-20 20:18:38.684 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 30 steps
2025-11-20 20:18:38.684 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Run_Program
2025-11-20 20:18:45.518 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 20 steps
2025-11-20 20:18:45.519 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Run_Program
2025-11-20 20:18:53.479 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 30 steps
2025-11-20 20:18:53.479 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Reformat
2025-11-20 20:19:01.593 | DEBUG    | services.stag.abinitio_logic_extractor:_parse_component_analysis_response:454 - Failed to parse AI component analysis: Expecting ',' delimiter: line 34 column 32 (char 4399)
2025-11-20 20:19:01.593 | WARNING  | services.stag.abinitio_logic_extractor:_analyze_component_deeply:426 -    ⚠ AI analysis incomplete for Reformat
2025-11-20 20:19:01.594 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Reformat
2025-11-20 20:19:10.680 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 30 steps
2025-11-20 20:19:10.680 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Reformat
2025-11-20 20:19:20.279 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 30 steps
2025-11-20 20:19:20.281 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: Reformat
2025-11-20 20:19:29.695 | DEBUG    | services.stag.abinitio_logic_extractor:_parse_component_analysis_response:454 - Failed to parse AI component analysis: Expecting ',' delimiter: line 35 column 45 (char 3501)
s
2025-11-20 20:20:20.804 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Partition\\Broadcast
2025-11-20 20:20:29.560 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 30 steps
2025-11-20 20:20:29.560 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Partition\\Broadcast
2025-11-20 20:20:39.375 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 28 steps
2025-11-20 20:20:39.375 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Join
2025-11-20 20:20:50.697 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 30 steps
2025-11-20 20:20:50.698 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Join
2025-11-20 20:21:00.216 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 25 steps
2025-11-20 20:21:00.216 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Join
2025-11-20 20:21:09.643 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:423 -    ✅ AI analysis: 25 steps
2025-11-20 20:21:09.643 | DEBUG    | services.stag.abinitio_logic_extractor:_analyze_component_deeply:355 -    🔍 Deep analyzing component: C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Join
