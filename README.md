Hadoop Workflows for Preprocessing Steps
1. Unique PermID Relations Creation
Workflow: cdd: family_clustering
Location: CodebaseIntelligence/hadoop_repos/hadoop_repos/app-cdd/workflows/family_clustering/oozie/workflow.xml
Action Name: get_unique_permid_relations (lines 136-154)
Script: famc_unique_permid_relations.py
Conditional Execution: Only runs when source notification is es_xref_famc (decision node at lines 127-134)
Purpose: Creates unique PermID relations from escan patientAccountsXpermId dataset, filters for matchind='HM', creates bidirectional relations
2. Policy Info Propagation
Workflow: cdd: family_clustering (same workflow)
Location: Same workflow file
Action Name: propagate_policyinfo_to_permid_relations (lines 156-177)
Script: famc_propagate_policyinfo_to_permid_relations.py
Execution: Always runs after get_unique_permid_relations (or directly if source is not es_xref_famc)
Purpose: Propagates policy information from LR xref transactions to PermID relations, creates candidate PermID transactions
Summary
Both preprocessing steps are in the Hadoop workflow cdd: family_clustering, which is separate from leadrepository : famc_xref_table_update.
cdd: family_clustering → Creates unique PermID relations and propagates policy info (preprocessing)
leadrepository : famc_xref_table_update → Creates the XREF PermID table (main processing)
In Databricks, pl_leadrepository_xref_famc combines both: it includes the preprocessing steps (unique PermID relations and policy propagation) and then creates the XREF table, all in one pipeline.
This explains why Databricks has these preprocessing steps while the Hadoop famc_xref_table_update workflow does not — they are handled by a separate Hadoop workflow (cdd: family_clustering).
