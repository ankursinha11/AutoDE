
1. File: 35. Hadoop_escanglobalmrn_vs_DB_pl_gmrn_ghic.xlsx
Sheet: Pipeline Comparison

Update Row 7 (Activity: gmrnghic):
Column Databricks Logic Details: Append this text:
"Logic includes joining on hospitalfk when fetching foundcoverage and globalmrnxpacct."

Sheet: Logic Comparison

Update Row 7 (Step: Generate Unique GHIC Records):
Column Databricks Activity Description: Append this text:
"The gmrnghic notebook explicitly uses hospitalfk as a join key with hospinsurancecodes and patientacctcodes to correctly map insurance policies."

2. File: 31. Hadoop_Escan_coveragehelper_medicare_helper_selectaccts vs DB pl_mbihelper.xlsx
Sheet: Pipeline Comparison

Update Row 6 (Activity: selectaccounts):
Column Databricks Logic Details: Add this text:
"selectaccounts notebook uses hospitalfk to identify and filter relevant accounts for processing."

Sheet: Logic Comparison

Update Row 8 (Step: Select Accounts):
Column Databricks Activity Description: Add this text:
"Ensures hospitalfk is used to shard and distribute the account selection process."

3. File: 34. Hadoop_DI_bigtables vs DB_DI_bigtables.xlsx
Sheet: Pipeline Comparison

Update Row 5 (hospitalimportpayment) & Row 7 (vendorknownohicoverages):
Column Databricks Logic Details: Update to include:
"Sqoop extraction splits data by hospitalfk (or hospitalimportpaymentpk) to parallelize ingestion."

Sheet: Logic Comparison

Update Row 5 (Step: Sqoop Import):
Column Databricks Activity Description: Add this text:
"Utilizes hospitalfk in the split-by logic for efficient parallel imports from the source system."

4. File: 33. Hadoo_escan_DI_vs_DB_DI_abi_Group1and2.xlsx
Sheet: Logic Comparison

Update Row 13 (Step: All fields (hospitalpurge filtering...)):
Column Databricks Activity Description: You can explicitly mention:
"Reconciliation logic (reconcile_table_delta) includes hospitalfk in the ordering key to ensure correct handling of updates and merges."
