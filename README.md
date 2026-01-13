================================================================================
MBI HELPER EXCEL - COPY-PASTE UPDATES FOR JOIN TYPE DIFFERENCE
================================================================================

ISSUE: Join type difference (LEFT JOIN vs no join) is mentioned but not clearly 
documented with data impact. This is a CRITICAL difference that may affect 
which accounts are selected.

================================================================================
UPDATE 1: OVERVIEW SHEET
================================================================================

LOCATION: Row 6, Column 5 (Notes/Comments)

CURRENT TEXT:
Same logical purpose but different selection logic: Hadoop uses hospital partner last response status for ordering, Databricks uses only admit date. Hadoop checks EDI 270 generate in queue flag, Databricks does not. Different output formats (CSV vs Parquet).

REPLACE WITH (COPY THIS):
Same logical purpose but different selection logic with CRITICAL data impact: Hadoop performs LEFT JOIN with hospital partner last response status (hosppartnerlst) table and uses lastrespreceiveddate as primary ordering criteria (ORDER BY lastrespreceiveddate DESC, then admitdate DESC). Databricks does NOT join with hosppartnerlst table at all (table read is commented out in code) and uses only admitdate for ordering (ORDER BY admitdate DESC only). Data Impact: (1) Accounts without matching hosppartnerlst records are included in Hadoop (LEFT JOIN preserves all trackhelper records) but ordering behavior differs in Databricks, (2) When hosppartnerlst records exist, Hadoop prioritizes accounts with most recent lastrespreceiveddate, while Databricks only considers admitdate, (3) This may result in different account selection when multiple accounts have the same demohash-hospital FK combination. Additional differences: Hadoop checks EDI 270 generate in queue flag, Databricks does not. Different output formats (CSV vs Parquet).

================================================================================
UPDATE 2: LOGIC COMPARISON SHEET
================================================================================

LOCATION: Row 7, Column 6 (Gaps/Notes)

CURRENT TEXT:
Same logical purpose but different selection logic: Hadoop uses hospital partner last response status for ordering, Databricks uses only admit date. Hadoop checks EDI 270 generate in queue flag, Databricks does not. Different output formats (CSV vs Parquet).

REPLACE WITH (COPY THIS):
CRITICAL DIFFERENCE - Join Type and Ordering Logic: Hadoop performs LEFT JOIN with hospital partner last response status (hosppartnerlst) table in the ROW_NUMBER window function query (line 62: "left join hosppartnerlst hp on h.hospitalfk=hp.hospitalfk and hp.edipartnerfk=h.edipartnerfk") and uses lastrespreceiveddate as primary ordering criteria (ORDER BY lastrespreceiveddate DESC, admitdate DESC). Databricks does NOT join with hosppartnerlst table at all (table read is commented out in code, lines 68 and 78) and uses only admitdate for ordering (ORDER BY admitdate DESC only, line 93). Data Impact: (1) Accounts without matching hosppartnerlst records are still included in Hadoop (LEFT JOIN preserves all trackhelper records), but in Databricks the absence of hosppartnerlst join means ordering is based solely on admitdate, (2) When hosppartnerlst records exist, Hadoop prioritizes accounts with most recent lastrespreceiveddate, while Databricks only considers admitdate, (3) This may result in different account selection when multiple accounts have the same demohash-hospital FK combination - Hadoop may select based on last response date, Databricks only on admit date. Additional differences: Hadoop checks EDI 270 generate in queue flag, Databricks does not. Different output formats (CSV vs Parquet).

================================================================================
UPDATE 3: DATABRICKS LOGIC SHEET
================================================================================

LOCATION: Row 10, Column 5 (Processing Logic)

CURRENT TEXT (partial):
Step 3: Joins tracking data with helper repository (simplified logic - no hospital partner last response status join).

ENHANCE TO (UPDATE Step 3):
Step 3: Joins tracking data (trackhelper) with helper repository (helperrepo) using INNER JOIN. CRITICAL NOTE: Hospital partner last response status (hosppartnerlst) table is NOT joined (table read is commented out in code, lines 68 and 78). This differs from Hadoop which performs LEFT JOIN with hosppartnerlst.

================================================================================
OPTIONAL: HADOOP LOGIC SHEET
================================================================================

LOCATION: Find the row with "select_helper_accounts" action, Column 5 (Processing Logic)

IF IT EXISTS, ENSURE IT INCLUDES:
Joins tracking data (trackhelper) with hospital partner last response status (hosppartnerlst) using LEFT JOIN on hospitalfk and edipartnerfk (line 62). Uses ROW_NUMBER window function partitioned by demohash and hcsystemfk, ordered by lastrespreceiveddate DESC (primary) and admitdate DESC (secondary) to select one account per combination (line 61).

================================================================================
SUMMARY OF CHANGES
================================================================================

1. Overview Sheet (Row 6, Column 5): 
   - Replace current Notes text with enhanced version that explicitly mentions LEFT JOIN vs no join and data impact

2. Logic Comparison Sheet (Row 7, Column 6):
   - Replace current Gaps/Notes text with detailed version including code line references and data impact

3. Databricks Logic Sheet (Row 10, Column 5):
   - Update Step 3 to explicitly state hosppartnerlst is NOT joined (commented out)

4. Hadoop Logic Sheet (if select_helper_accounts exists):
   - Verify LEFT JOIN is mentioned in the processing logic

================================================================================
EVIDENCE FROM SOURCE CODE
================================================================================

Hadoop (selectaccounts.py):
- Line 62: "left join hosppartnerlst hp on h.hospitalfk=hp.hospitalfk and hp.edipartnerfk=h.edipartnerfk"
- Line 61: ORDER BY uses "hp.lastrespreceiveddate" first, then "hr.admitdate"

Databricks (coveragehelpers/selectaccounts.py):
- Line 68: hosppartnerlst read is commented out: "# hosppartnerlst = spark.read.parquet(...)"
- Line 78: hosppartnerlst temp view is commented out: "# hosppartnerlst.createOrReplaceTempView(...)"
- Line 94: Only joins trackhelper with helperrepo (no hosppartnerlst)
- Line 93: ORDER BY uses only "hr.admitdate" (no lastrespreceiveddate)

================================================================================
