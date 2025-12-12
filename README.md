MEDICAID LEADS PIPELINE - FLOW DIAGRAM
========================================

Data Flow Diagram - Databricks                    |  Data Flow Diagram - Hadoop
---------------------------------------------------|---------------------------------------------------
1. Get Notification & Initialize                   |  1. Get Notification & Initialize
   (update_notification_inprogress activity)       |     (get_notification action executes get_notification.py)
                                                   |
2. Log Workflow Start                              |  2. Log Workflow Start
   (360_logger_v1_Running activity)               |     (check_previous_wf_status action executes oozie_360_wf_checker.py)
                                                   |     (restart_previous_failed_wf action executes oozie_360_wf_runner_v1.sh)
                                                   |     (check_notification action executes check_notification_v3.0.sh)
                                                   |     (get_date action executes get_datetime.sh)
                                                   |     (oozie_360_log_start action executes oozie_360_logger_v1.py)
                                                   |
3. Create Admit Date Lookup                       |  3. Create Admit Date Lookup
   (mcaid_get_minmaxdt activity                   |     (get_minmaxdt action executes
    executes mcaid_createlookup_maxminadmitdays.py)|      mcaid_createlookup_maxminadmitdays.py)
                                                   |
4. Get Candidate Patient Accounts                 |  4. Get Candidate Patient Accounts
   (mcaid_get_candidate_patientaccts activity     |     (get_candidate_patientaccts action executes
    executes mcaid_get_candidate_patientaccts.py) |      mcaid_get_candidate_patientaccts.py)
                                                   |
5. (Not present - merged into lead lookup)        |  5. Process CPA-LSB Cross-Table
                                                   |     (process_cpa_lsb_xtable action executes cpa_lsb_xtable.py)
                                                   |
6. Lead Lookup by Identity (Parallel)             |  6. Lead Lookup by Identity (Parallel Fork)
   (lsb_lookup activity                           |     (process_globalmrnifk_leadlookup action executes
    executes optimized_lsb_lookup.py for all      |      run_leadlookup.sh with globalmrnifk)
    identity types: GlobalMRNIFK, PermId, SSN,    |     (process_permid_leadlookup action executes
    MedicalRecNum, ClusteredAcctFK)               |      run_leadlookup.sh with permid)
                                                   |     (process_ssn_leadlookup action executes
                                                   |      run_leadlookup.sh with ssn)
                                                   |     (process_medicalrecnum_leadlookup action executes
                                                   |      run_leadlookup.sh with medicalrecnum)
                                                   |     (process_clusteredacctfk_leadlookup action executes
                                                   |      run_leadlookup.sh with clusteredacctfk)
                                                   |
7. (Not present - merged into lead lookup)        |  7. Merge CPA X Leads
                                                   |     (merge_cpa_xleads action executes merge_cpa_xleads.py)
                                                   |
8. Process Leads                                  |  8. Process Leads
   (mcaid_process_leads activity                  |     (process_leads action executes
    executes mcaid_process_leads.py)              |      process_leads_medicaid.py)
                                                   |
9. Check Leads & Export                           |  9. Check Leads & Export
   (processlead_exists activity checks            |     (hdfs_dir_check action executes hdfs_dir_check.sh)
    output directory)                             |     (check_leads action executes check_leads.py)
   (mcaid_check_leads activity                    |     (hdfs_dir_check_leads action executes hdfs_dir_check.sh)
    executes check_leads.py)                      |     (sqoop_out action executes Sqoop export to
   (mcaid_sqoop_out activity                      |      hdppatientacctxlead table)
    executes sqoop_out.py via JDBC)               |     (sqoop_out_hdpbatch action executes Sqoop export to
                                                   |      hdppatientacctxleadbatch table)
                                                   |
10. Database Sharding                             |  10. (Not present in Hadoop)
    (db_sharding activity                         |
     executes db_sharding_to_adls.py)             |
    (db_sharding_move activity                    |
     executes db_sharding_move.py)                |
                                                   |
11. Update Notification & Cleanup                |  11. Update Notification & Cleanup
    (update_notification_processed activity)      |      (update_notification action executes update_notification.py)
    (delete_trigger_file activity)                |      (log_maprdb_success action executes lr_logfailure.py)
                                                   |      (log_notification action executes notification_log.sh)
                                                   |      (email_notify action sends email notification)
                                                   |      (purge_intermediate_data action executes delete_bcdata.sh)
