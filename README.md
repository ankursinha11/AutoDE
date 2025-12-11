1. CRITICAL: edidatasourcefk Value Mismatch (Medicare)
CRITICAL DATA INTEGRITY ISSUE: Hadoop hardcodes edidatasourcefk='41' for Medicare leads, while Databricks hardcodes edidatasourcefk='49'. This value is used as a JOIN key in downstream maintenance and audit scripts (hospital_low_counts_createdate.py, monthly_report_xref_leadsource.py, etc.) when joining hdppatientacctxlead with ediglobalsourceddata. If ediglobalsourceddata table in SQL Server contains EDIDataSourceFK='41' but Databricks writes '49', the JOIN will fail and Medicare leads will be excluded from downstream reports, causing data loss. VERIFICATION REQUIRED: Query SQL Server table eScan.dbo.ediglobalsourceddata to confirm which EDIDataSourceFK values exist for Medicare leads. If only '41' exists, Databricks code must be changed from '49' to '41'. If '49' exists, Databricks is correct. Evidence: Hadoop process_leads_medicare.py line 122 sets lit('41'), Databricks process_leads_medicare.py line 119 sets lit('49').
2. Additional DOD Filter in Databricks
DATA INCREASE: Databricks includes an additional filter condition that was not present in Hadoop. Hadoop filters candidate accounts using only hasmedicareflag == 0. Databricks uses (hasmedicareflag == 0) | (dod.isNotNull()), meaning it includes accounts where hasmedicareflag == 0 OR date of death is not null. This OR condition means Databricks will process MORE candidate accounts than Hadoop, as it includes deceased patient accounts even if hasmedicareflag != 0. This will result in INCREASED lead counts in Databricks compared to Hadoop. Impact: Data volume will be higher in Databricks. If this is intentional business logic (to include deceased accounts), no action needed. If parity with Hadoop is required, remove the DOD condition. Evidence: Hadoop mcare_get_candidate_patientaccts.py line 60 uses only hasmedicareflag == 0, Databricks mcare_get_candidate_patientaccnts.py line 246 uses (hasmedicareflag == 0) | (dod.isNotNull()).
3. Time Window Difference (enddt) for lead_mode=1
TIME WINDOW DIFFERENCE: Hadoop hardcodes the time window to 3 days (enddt = date_sub(current_date(), 3)) for filtering patient accounts in lead_mode=1. Databricks uses a configurable parameter num_of_days that defaults to 90 days (enddt = date_sub(current_date(), int(num_of_days))). This means Databricks processes patient accounts updated in the last 90 days by default, compared to Hadoop's 3 days. Impact: Databricks will process significantly MORE accounts in lead_mode=1, potentially leading to higher lead counts. This could be intentional to capture more historical data, or it may need to be adjusted to 3 days for parity with Hadoop. Evidence: Hadoop mcare_get_candidate_patientaccts.py line 48 hardcodes date_sub(current_date(), 3), Databricks mcare_get_candidate_patientaccnts.py line 215 uses date_sub(current_date(), int(num_of_days)) where num_of_days defaults to "90" in pl_leaddiscovery_medicareleads.json line 1561.
4. AID Module - New Functionality in Databricks
NEW FUNCTIONALITY IN DATABRICKS: AID (Access Information Database) module is additional functionality in Databricks that did not exist in Hadoop. AID processes a separate subset of patient accounts from aidpatientaccts and patientacctsaccesscoordinator tables (likely Epic/Coordinator systems). AID accounts are processed through separate pipelines (pl_leaddiscovery_medicareleads_aid.json) and tracked with different identifiers: edidatasourcefk='51' (vs regular Medicare '49'), breadcrumb suffix '-AD' (e.g., '20250111T13-AD'), and separate output paths. This does not affect regular Medicare/Medicaid lead processing as AID runs as a separate pipeline. Evidence: Databricks has AID-specific pipelines and code branches (if module=="aid"), while Hadoop repos show no AID functionality.
Summary of All Differences:
edidatasourcefk: '41' (Hadoop) vs '49' (Databricks) — CRITICAL — may cause data loss
DOD filter: Additional in Databricks — increases data volume
Time window: 3 days (Hadoop) vs 90 days default (Databricks) — increases data volume
AID module: New in Databricks — separate functionality, no impact on regular processing
Copy and paste these into your Excel sheet as needed.
Candidate Patient Account Selection - Complete Row:
Column 1 (Stage Name):
Stage 3: Candidate Patient Account Selection
Stage 3: Candidate Patient Account Selection
Column 2 (Hadoop Logic):
The workflow identifies candidate patient accounts that are eligible for Medicare lead processing. It reads patient accounts from Delta Lake, filters for accounts with total charges > 0, applies admit date filters using the lookup tables, and filters for accounts where hasmedicareflag = 0 (accounts not yet processed for Medicare). When lead_mode_flag='1', it filters patient accounts based on trgupdatedate or vsnapflags_createdate >= enddt where enddt is hardcoded to 3 days (date_sub(current_date(), 3)). It then enriches accounts with Global MRN and PermId from reference data, validates identities, and writes candidate accounts to output.
Column 3 (Databricks Logic):
The pipeline performs the same candidate selection. It reads patient accounts, applies the same filters (total charges > 0, admit date ranges, hasmedicareflag = 0). Additionally, it includes accounts where DOD (date of death) is not null. When lead_mode_flag='1', it filters patient accounts based on trgupdatedate or vsnapflags_createdate >= enddt where enddt uses a configurable parameter num_of_days that defaults to 90 days (date_sub(current_date(), int(num_of_days))) instead of Hadoop's hardcoded 3 days. It enriches with Global MRN and PermId, validates identities, and writes candidates. The pipeline also supports an 'aid' module mode for AID-specific patient account processing.
Column 4 (Review/Comments):
⚠ Review - Multiple differences: (1) Additional DOD filter in Databricks includes accounts where dod.isNotNull(), increasing data volume. (2) Time window difference: When lead_mode=1, Hadoop uses 3 days (hardcoded) vs Databricks uses 90 days (configurable default), processing significantly more accounts. (3) AID module support is new functionality in Databricks.
⚠ Review - Multiple differences: (1) Additional DOD filter in Databricks includes accounts where dod.isNotNull(), increasing data volume. (2) Time window difference: When lead_mode=1, Hadoop uses 3 days (hardcoded) vs Databricks uses 90 days (configurable default), processing significantly more accounts. (3) AID module support is new functionality in Databricks.










MEDICARE LEADS PIPELINE - FLOW DIAGRAM
========================================

Data Flow Diagram - Databricks                    |  Data Flow Diagram - Hadoop
---------------------------------------------------|---------------------------------------------------
1. Get Notification & Initialize                   |  1. Get Notification & Initialize
   (update_notification_inprogress activity)       |     (get_notification action executes get_notification.py)
                                                   |
2. Log Workflow Start                              |  2. Log Workflow Start
   (360_logger_v1_Running activity)               |     (check_previous_wf_status action executes oozie_wf_checker.py)
                                                   |     (restart_previous_failed_wf action executes oozie_wf_runner.sh)
                                                   |     (check_notification action executes check_notification_v3.0.sh)
                                                   |     (get_date action executes get_datetime.sh)
                                                   |
3. Create Admit Date Lookup                       |  3. Create Admit Date Lookup
   (mcare_createlookup_maxminadmitdays activity    |     (get_minmaxdt action executes 
    executes mcare_createlookup_maxminadmitdays.py)|      mcare_createlookup_maxminadmitdays.py)
                                                   |
4. Get Candidate Patient Accounts                 |  4. Get Candidate Patient Accounts
   (mcare_get_candidate_patientaccts activity      |     (get_candidate_patientaccts action executes
    executes mcare_get_candidate_patientaccnts.py) |      mcare_get_candidate_patientaccts.py)
                                                   |
5. Process CPA-LSB Cross-Table                    |  5. Process CPA-LSB Cross-Table
   (mcare_process_cpa_lsb_xtable activity         |     (process_cpa_lsb_xtable action executes
    executes cpa_lsb_xtable_medicare.py)          |      cpa_lsb_xtable_medicare.py)
                                                   |
6. Lead Lookup by Identity (Parallel)             |  6. Lead Lookup by Identity (Parallel Fork)
   (mcare_process_globalmrnifk activity           |     (process_globalmrnifk_leadlookup action executes
    executes LeadLookupByID.py with globalmrnifk) |      run_leadlookup.sh with globalmrnifk)
   (mcare_process_permid activity                 |     (process_permid_leadlookup action executes
    executes LeadLookupByID.py with permid)        |      run_leadlookup.sh with permid)
   (mcare_process_ssn activity                    |     (process_ssn_leadlookup action executes
    executes LeadLookupByID.py with ssn)          |      run_leadlookup.sh with ssn)
   (mcare__medicalrecnum activity                 |     (process_medicalrecnum_leadlookup action executes
    executes LeadLookupByID.py with medicalrecnum)|      run_leadlookup.sh with medicalrecnum)
   (mcare_process_clusteredacctfk activity        |     (process_clusteredacctfk_leadlookup action executes
    executes LeadLookupByID.py with clusteredacctfk)|     run_leadlookup.sh with clusteredacctfk)
                                                   |
7. Process Leads                                  |  7. Process Leads
   (mcare_process_leads activity                  |     (process_leads action executes
    executes process_leads_medicare.py)           |      process_leads_medicare.py)
                                                   |
8. Check Leads & Export                           |  8. Check Leads & Export
   (Check whether process lead output exist       |     (hdfs_dir_check action executes hdfs_dir_check.sh)
    or not activity checks output directory)      |     (check_leads action executes check_leads.py)
   (mcare_check_leads activity                    |     (hdfs_dir_check_leads action executes hdfs_dir_check.sh)
    executes check_leads.py)                     |     (sqoop_out action executes Sqoop export to
   (mcare_sqoop_out activity                      |      hdppatientacctxlead table)
    executes sqoop_out.py via JDBC)               |     (sqoop_out_hdpbatch action executes Sqoop export to
                                                   |      hdppatientacctxleadbatch table)
                                                   |
9. Database Sharding                              |  9. (Not present in Hadoop)
   (db_sharding activity                          |
    executes db_sharding_to_adls.py)              |
   (db_sharding_move activity                     |
    executes db_sharding_move.py)                 |
                                                   |
10. Update Notification & Cleanup                |  10. Update Notification & Cleanup
    (update_notification_completed activity)       |      (update_notification action executes update_notification.py)
    (delete_trigger_file activity)                |      (log_maprdb_success action executes lr_logfailure.py)
                                                   |      (log_notification action executes notification_log.sh)
                                                   |      (email_notify action sends email notification)
                                                   |      (purge_intermediate_data action executes delete_bcdata.sh)
