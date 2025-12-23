:761 - ✓ Similarity Agent initialized
2025-12-23 19:11:22.873 | INFO     | services.metadata_extractor:get_tables:50 - 📊 Extracting tables from hadoop...
2025-12-23 19:11:23.219 | INFO     | services.metadata_extractor:get_tables:79 -   ✓ Found 375 unique tables in hadoop
2025-12-23 19:12:37.662 | INFO     | services.langgraph.workflow:query:152 - Processing query: can you tell me in detail about: Databricks Pipeline:  pl_leaddiscovery_lead_propagation
2025-12-23 19:12:37.662 | INFO     | services.langgraph.memory:__init__:87 - Initialized conversation memory for session session_20251223_191237
2025-12-23 19:12:37.665 | INFO     | services.langgraph.workflow:_analyze_query_node:230 - Node: Analyze Query
2025-12-23 19:12:37.666 | INFO     | services.langgraph.workflow:_analyze_query_node:252 -   Intent: QueryIntent.GENERAL_QUESTION, Systems: [<SystemType.DATABRICKS: 'databricks'>], Filter: pl_leaddiscovery_lead_propagation
2025-12-23 19:12:37.667 | INFO     | services.langgraph.workflow:_retrieve_node:271 - Node: Retrieve
2025-12-23 19:12:37.667 | DEBUG    | services.retrieval.query_rewriter:rewrite_query:90 - Rewrote query: 'can you tell me in detail about: Databricks Pipeline:  pl_leaddiscovery_lead_propagation' -> 'can you tell me in detail about: Databricks Pipeline:  pl_leaddiscovery_lead_propagation databricks notebook databricks databricks pipeline'
2025-12-23 19:12:37.983 | DEBUG    | services.retrieval.reranker:rerank:133 - Reranked 5 documents -> 5 results
2025-12-23 19:12:37.984 | INFO     | services.langgraph.workflow:_retrieve_node:335 -   Retrieved 5 documents
2025-12-23 19:12:37.985 | INFO     | services.langgraph.workflow:_read_files_node:355 - Node: Read Files
2025-12-23 19:12:37.985 | INFO     | services.langgraph.workflow:_read_files_node:383 -   Reading 4 files
2025-12-23 19:12:37.987 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adf\pipeline\pl_leaddiscovery_lead_propagation.json (1154 lines, json)
2025-12-23 19:12:37.989 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adf\pipeline\pl_leaddiscovery_globalmrn_assign_aid.json (1168 lines, json)
2025-12-23 19:12:37.990 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adf\pipeline\pl_leadrepo_escan_import_fc.json (703 lines, json)
2025-12-23 19:12:37.992 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adf\pipeline\pl_leaddiscovery_leadlookup_knowncommercial.json (1411 lines, json)
2025-12-23 19:12:37.992 | INFO     | services.langgraph.workflow:_read_files_node:425 -   Read 4 files successfully (0 failed)
2025-12-23 19:12:37.993 | INFO     | services.langgraph.workflow:_analyze_code_node:447 - Node: Analyze Code
2025-12-23 19:12:37.993 | INFO     | services.langgraph.workflow:_analyze_code_node:502 -   Analyzed 0 files (0 failed)
2025-12-23 19:12:37.994 | INFO     | services.langgraph.workflow:_generate_response_node:525 - Node: Generate Response
2025-12-23 19:12:49.370 | INFO     | services.langgraph.workflow:_generate_response_node:550 -   Generated response (confidence: 0.70)
2025-12-23 19:12:49.371 | DEBUG    | services.langgraph.memory:add_turn:124 - Added user turn: can you tell me in detail about: Databricks Pipeli...
2025-12-23 19:12:49.371 | DEBUG    | services.langgraph.memory:add_turn:124 - Added assistant turn: Certainly! Here’s a detailed analysis of the **Dat...
2025-12-23 19:12:49.380 Please replace `use_container_width` with `width`.

`use_container_width` will be removed after 2025-12-31.

For `use_container_width=True`, use `width='stretch'`. For `use_container_width=False`, use `width='content'`.
2025-12-23 19:14:36.938 | INFO     | services.langgraph.workflow:query:152 - Processing query: Can you find this logic: "if mode1 == 'RUNNING':
    status = 'RUNNING'
    current_dt = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    start_time = str(current_dt)
    end_time = ''
    wf_run_time = ''
    duration = ''
    df = readfromcosmosdb(cfg)
    if not 'runstatusid' in df.columns:
        cflag = '0'
    else:
        cflag = '1'
    if len(df.head(1)) == 0:
        rerun = 0
    else:
        df_row = df.collect()[0]
        project = df_row['project']
        module = df_row['module']
        bcdata = df_row['bcdata']
        user_id = df_row['user_id']
        rerun = df_row['rerun']
        rerun = int(rerun) + 1
        if cflag == '1':
            runstatusid = str(df_row['runstatusid'])
        else:
            runstatusid = '0'"

In any databricks notebook in pl_leaddiscovery_lead_propagation  pipeline
2025-12-23 19:14:36.940 | INFO     | services.langgraph.workflow:_analyze_query_node:230 - Node: Analyze Query
2025-12-23 19:14:36.941 | INFO     | services.langgraph.workflow:_analyze_query_node:252 -   Intent: QueryIntent.COLUMN_TRANSFORMATION, Systems: [<SystemType.DATABRICKS: 'databricks'>], Filter: if mode1 == 'RUNNING':
    status = 'RUNNING'
    current_dt = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    start_time = str(current_dt)
    end_time = ''
    wf_run_time = ''
    duration = ''
    df = readfromcosmosdb(cfg)
    if not 'runstatusid' in df.columns:
        cflag = '0'
    else:
        cflag = '1'
    if len(df.head(1)) == 0:
        rerun = 0
    else:
        df_row = df.collect()[0]
        project = df_row['project']
        module = df_row['module']
        bcdata = df_row['bcdata']
        user_id = df_row['user_id']
        rerun = df_row['rerun']
        rerun = int(rerun) + 1
        if cflag == '1':
            runstatusid = str(df_row['runstatusid'])
        else:
            runstatusid = '0'
2025-12-23 19:14:36.942 | INFO     | services.langgraph.workflow:_retrieve_node:271 - Node: Retrieve
2025-12-23 19:14:36.942 | DEBUG    | services.retrieval.query_rewriter:rewrite_query:90 - Rewrote query: 'Can you find this logic: "if mode1 == 'RUNNING':
    status = 'RUNNING'
    current_dt = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    start_time = str(current_dt)
    end_time = ''
    wf_run_time = ''
    duration = ''
    df = readfromcosmosdb(cfg)
    if not 'runstatusid' in df.columns:
        cflag = '0'
    else:
        cflag = '1'
    if len(df.head(1)) == 0:
        rerun = 0
    else:
        df_row = df.collect()[0]
        project = df_row['project']
        module = df_row['module']
        bcdata = df_row['bcdata']
        user_id = df_row['user_id']
        rerun = df_row['rerun']
        rerun = int(rerun) + 1
        if cflag == '1':
            runstatusid = str(df_row['runstatusid'])
        else:
            runstatusid = '0'"

In any databricks notebook in pl_leaddiscovery_lead_propagation  pipeline' -> 'Can you find this logic: "if mode1 == 'RUNNING':
    status = 'RUNNING'
    current_dt = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    start_time = str(current_dt)
    end_time = ''
    wf_run_time = ''
    duration = ''
    df = readfromcosmosdb(cfg)
    if not 'runstatusid' in df.columns:
        cflag = '0'
    else:
        cflag = '1'
    if len(df.head(1)) == 0:
        rerun = 0
    else:
        df_row = df.collect()[0]
        project = df_row['project']
        module = df_row['module']
        bcdata = df_row['bcdata']
        user_id = df_row['user_id']
        rerun = df_row['rerun']
        rerun = int(rerun) + 1
        if cflag == '1':
            runstatusid = str(df_row['runstatusid'])
        else:
            runstatusid = '0'"

In any databricks notebook in pl_leaddiscovery_lead_propagation  pipeline databricks pipeline attribute databricks notebook field databricks column'
2025-12-23 19:14:38.036 | DEBUG    | services.retrieval.reranker:rerank:133 - Reranked 15 documents -> 15 results
2025-12-23 19:14:38.036 | INFO     | services.langgraph.workflow:_retrieve_node:335 -   Retrieved 15 documents
2025-12-23 19:14:38.037 | INFO     | services.langgraph.workflow:_read_files_node:355 - Node: Read Files
2025-12-23 19:14:38.037 | INFO     | services.langgraph.workflow:_read_files_node:383 -   Reading 13 files
2025-12-23 19:14:38.039 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\HINTSDISCOVERY\HintsGeneration\Common\hints_get_candidate_patientaccts.py (139 lines, python)
2025-12-23 19:14:38.040 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adhoc\Cleanup\HFC_hipaa_issue_fc_leads_cleanup.py (215 lines, python)
2025-12-23 19:14:38.041 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\LeadDiscovery\common\createlookup_maxminadmitdays.py (147 lines, python)
2025-12-23 19:14:38.042 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adhoc\fc hitstatus 0\lsb_cleanup_mh_all_ids.py (377 lines, python)
2025-12-23 19:14:38.044 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\LeadDiscovery\subdob_datamining\subdob_datamine_func_fc_coverageid.py (129 lines, python)
2025-12-23 19:14:38.044 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adhoc\Prodfix\chc_gmrnidxref_cleanup.py (33 lines, python)
2025-12-23 19:14:38.046 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adhoc\fc hitstatus 0\lsb_cleanup_hitststatus_mrn.py (260 lines, python)
2025-12-23 19:14:38.047 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\dataingestion\bigtables\fetch_last_value_uc.py (79 lines, python)
2025-12-23 19:14:38.048 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\LeadDiscovery\subdob_datamining\import_edisubscriberdobsearch.py (117 lines, python)
2025-12-23 19:14:38.049 | DEBUG    | services.analysis.file_reader:read_file:229 - Read file: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\LeadDiscovery\common\process_leads.py (1002 lines, python)
2025-12-23 19:14:38.049 | INFO     | services.langgraph.workflow:_read_files_node:425 -   Read 10 files successfully (0 failed)
2025-12-23 19:14:38.050 | INFO     | services.langgraph.workflow:_analyze_code_node:447 - Node: Analyze Code
2025-12-23 19:14:38.051 | INFO     | services.langgraph.workflow:_analyze_code_node:468 -   Analyzing 10 files
2025-12-23 19:14:38.051 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\HINTSDISCOVERY\HintsGeneration\Common\hints_get_candidate_patientaccts.py
2025-12-23 19:14:38.051 | INFO     | services.analysis.code_analyzer:analyze_file:116 - Analyzing file: HINTSDISCOVERY\HintsGeneration\Common\hints_get_candidate_patientaccts.py (python)
2025-12-23 19:14:38.051 | ERROR    | services.analysis.code_analyzer:_analyze_python:232 - Error analyzing Python file: AIScriptAnalyzer.analyze_code() got an unexpected keyword argument 'custom_prompt'
2025-12-23 19:14:38.051 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adhoc\Cleanup\HFC_hipaa_issue_fc_leads_cleanup.py
2025-12-23 19:14:38.052 | INFO     | services.analysis.code_analyzer:analyze_file:116 - Analyzing file: adhoc\Cleanup\HFC_hipaa_issue_fc_leads_cleanup.py (python)
2025-12-23 19:14:38.052 | ERROR    | services.analysis.code_analyzer:_analyze_python:232 - Error analyzing Python file: AIScriptAnalyzer.analyze_code() got an unexpected keyword argument 'custom_prompt'
2025-12-23 19:14:38.052 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\LeadDiscovery\common\createlookup_maxminadmitdays.py
2025-12-23 19:14:38.052 | INFO     | services.analysis.code_analyzer:analyze_file:116 - Analyzing file: LeadDiscovery\common\createlookup_maxminadmitdays.py (python)
2025-12-23 19:14:38.052 | ERROR    | services.analysis.code_analyzer:_analyze_python:232 - Error analyzing Python file: AIScriptAnalyzer.analyze_code() got an unexpected keyword argument 'custom_prompt'
2025-12-23 19:14:38.053 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\adhoc\fc hitstatus 0\lsb_cleanup_mh_all_ids.py
2025-12-23 19:14:38.053 | INFO     | services.analysis.code_analyzer:analyze_file:116 - Analyzing file: adhoc\fc hitstatus 0\lsb_cleanup_mh_all_ids.py (python)
2025-12-23 19:14:38.053 | ERROR    | services.analysis.code_analyzer:_analyze_python:232 - Error analyzing Python file: AIScriptAnalyzer.analyze_code() got an unexpected keyword argument 'custom_prompt'
2025-12-23 19:14:38.053 | DEBUG    | services.analysis.file_reader:read_file:175 - Using cached content for: C:\Users\Ankur.Sinha\Downloads\CodebaseIntelligencev2\CodebaseIntelligence\LeadDiscovery\subdob_datamining\subdob_datamine_func_fc_coverageid.py
2025-12-23 19:14:38.053 | INFO     | services.analysis.code_analyzer:analyze_file:116 - Analyzing file: LeadDiscovery\subdob_datamining\subdob_datamine_func_fc_coverageid.py (python)
2025-12-23 19:14:38.053 | ERROR    | services.analysis.code_analyzer:_analyze_python:232 - Error analyzing Python file: AIScriptAnalyzer.analyze_code() got an unexpected keyword argument 'custom_prompt'
2025-12-23 19:14:38.054 | INFO     | services.langgraph.workflow:_analyze_code_node:502 -   Analyzed 5 files (0 failed)
2025-12-23 19:14:38.054 | INFO     | services.langgraph.workflow:_generate_response_node:525 - Node: Generate Response
2025-12-23 19:14:43.348 | INFO     | services.langgraph.workflow:_generate_response_node:550 -   Generated response (confidence: 1.00)
2025-12-23 19:14:43.348 | DEBUG    | services.langgraph.memory:add_turn:124 - Added user turn: Can you find this logic: "if mode1 == 'RUNNING':
 ...
2025-12-23 19:14:43.348 | DEBUG    | services.langgraph.memory:add_turn:124 - Added assistant turn: Based on the provided code context from the files ...
2025-12-23 19:14:43.369 Please replace `use_container_width` with `width`.

`use_container_width` will be removed after 2025-12-31.

For `use_container_width=True`, use `width='stretch'`. For `use_container_width=False`, use `width='content'`.
