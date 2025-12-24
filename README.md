Updated text for Excel
Column 1 (Stage):
Stage 11: Additional Source Types (Architectural Differences)
Column 2 (Hadoop Flow Description):
CHC (chc_xref_lsb) is handled by a separate workflow (chc_leadservicebase_create) in Hadoop, not part of the main "leadservicebase : lead gen and update" workflow. HFC, MH, and Family Clustering variants do not exist in Hadoop.
Column 3 (Databricks Flow Description):
pl_leadservicebase Switch activity executes child pipelines for CHC (chc_xref_lsb), HFC (hfc_xref_lsb), MH (mh_xref_lsb), and Family Clustering variants (es_lsb_famc, fc_lsb_famc, ie_lsb_famc). CHC exists in both platforms but with different architecture (separate workflow in Hadoop vs child pipeline in Databricks). HFC, MH, and FAMC are new features in Databricks.
Column 4 (Match Status):
Architectural Difference / Databricks Only
Column 5 (Notes/Comments):
CHC processing exists in both platforms but uses different architecture: Hadoop has a separate workflow (chc_leadservicebase_create) while Databricks uses a child pipeline (pl_leadservicebase_chc) from the main pipeline. HFC, MH, and FAMC are new Databricks-only features. These are not part of the detailed comparison scope which focuses on the main workflow mapping to pl_leadservicebase and pl_leadservicebase_gmrnmerge only.
