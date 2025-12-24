 hospitalfk Differences - Update ALL THREE Sheets

## Summary
You want to document the `hospitalfk` differences in **all three places** so clients can find it anywhere they look. Good idea!

---

## ✅ WHAT TO UPDATE

### 1. **Logic Comparison Sheet - Row 10** (REQUIRED)
### 2. **Hadoop Logic Sheet - Row 17** (REQUIRED)  
### 3. **Databricks Logic Sheet - Row 11** (REQUIRED)

---

## 📝 EXACT TEXT TO ADD

---

## 1. LOGIC COMPARISON SHEET - Row 10

**Column 2 (Hadoop Action Description) - ADD AT THE END:**
```
Best lead selection: ROW_NUMBER deduplication with PARTITION BY patientacctifk,lsb_edipartnerfk,lsb_coverageid,lsb_id (NO hospitalfk). Deduplicates across all hospitals per patient account.
```

**Column 3 (Databricks Activity Description) - ADD AT THE END:**
```
Best lead selection: ROW_NUMBER deduplication with PARTITION BY patientacctifk,hospitalfk,lsb_edipartnerfk,lsb_coverageid,lsb_id (INCLUDES hospitalfk). Deduplicates separately per hospital per patient account. Also uses upper(trim(lsb_coverageid)) for coverage ID normalization.
```

**Column 5 (Gaps/Notes) - ADD AT THE END:**
```
CRITICAL: Best lead selection deduplication difference - Databricks includes hospitalfk in PARTITION BY (both ROW_NUMBER windows), Hadoop does not. Hadoop deduplicates across ALL hospitals per patient account (one lead selected), while Databricks deduplicates separately PER HOSPITAL per patient account (one lead per hospital). This can result in different number of leads and different lead selection when patient accounts have leads from multiple hospitals.
```

---

## 2. HADOOP LOGIC SHEET - Row 17, Column 6

**Current text says:**
```
Step 14: Performs best lead selection - ROW_NUMBER by (patientacctifk, lsb_edipartnerfk, lsb_coverageid, lsb_id) ORDER BY lsb_demoupdatedate DESC, demohash DESC, then by keyvalue ASC, edidatasourcetablekey.
```

**REPLACE Step 14 with this (more accurate):**
```
Step 14: Performs best lead selection - Uses ROW_NUMBER window function for deduplication. First window: PARTITION BY patientacctifk,lsb_edipartnerfk,lsb_coverageid,lsb_id (NO hospitalfk) ORDER BY lsb_demoupdatedate DESC, demohash DESC. Second window: PARTITION BY patientacctifk,lsb_edipartnerfk,lsb_coverageid,rn ORDER BY keyvalue ASC, edidatasourcetablekey. Does NOT include hospitalfk in PARTITION BY, meaning deduplication happens across all hospitals for the same patient account (one best lead selected per patient account regardless of hospital).
```

**OR if you prefer to just ADD at the end (easier):**

**ADD THIS at the end of Column 6 (after Step 15):**
```
Step 14 DETAIL: Best lead selection uses ROW_NUMBER with PARTITION BY patientacctifk,lsb_edipartnerfk,lsb_coverageid,lsb_id (NO hospitalfk). This means deduplication happens across ALL hospitals per patient account - if a patient account has leads from Hospital A and Hospital B, only ONE best lead is selected (across both hospitals).
```

---

## 3. DATABRICKS LOGIC SHEET - Row 11, Column 6

**Current text says:**
```
Step 17: Performs best lead selection - ROW_NUMBER by (patientacctifk, lsb_edipartnerfk, lsb_coverageid, lsb_id) ORDER BY lsb_demoupdatedate DESC, demohash DESC, then by keyvalue ASC, edidatasourcetablekey.
```

**REPLACE Step 17 with this (more accurate):**
```
Step 17: Performs best lead selection - Uses ROW_NUMBER window function for deduplication. First window: PARTITION BY patientacctifk,hospitalfk,lsb_edipartnerfk,lsb_coverageid,lsb_id (INCLUDES hospitalfk) ORDER BY lsb_demoupdatedate DESC, demohash DESC, lsb_leadsourcevalue ASC, lsb_xrefsourcevalue ASC. Second window: PARTITION BY patientacctifk,hospitalfk,lsb_edipartnerfk,upper(trim(lsb_coverageid)),rn ORDER BY keyvalue ASC, edidatasourcetablekey. INCLUDES hospitalfk in PARTITION BY, meaning deduplication happens separately per hospital for the same patient account (one best lead selected per patient account per hospital). Also normalizes coverage ID with upper(trim()).
```

**OR if you prefer to just ADD at the end (easier):**

**ADD THIS at the end of Column 6 (after Step 18):**
```
Step 17 DETAIL: Best lead selection uses ROW_NUMBER with PARTITION BY patientacctifk,hospitalfk,lsb_edipartnerfk,lsb_coverageid,lsb_id (INCLUDES hospitalfk). This means deduplication happens separately PER HOSPITAL per patient account - if a patient account has leads from Hospital A and Hospital B, TWO best leads are selected (one from each hospital). Also uses upper(trim(lsb_coverageid)) for coverage ID normalization in second window.
```

---

## 🎯 RECOMMENDED APPROACH

### Option A: Replace the Step (More Accurate)
- **Hadoop Logic Row 17:** Replace Step 14 text with the detailed version above
- **Databricks Logic Row 11:** Replace Step 17 text with the detailed version above

### Option B: Add Detail at End (Easier)
- **Hadoop Logic Row 17:** Add "Step 14 DETAIL" at the end
- **Databricks Logic Row 11:** Add "Step 17 DETAIL" at the end

**I recommend Option B** - it's easier and doesn't require finding/replacing text in the middle of a long description.

---

## 📋 QUICK CHECKLIST

- [ ] **Logic Comparison Sheet - Row 10**
  - [ ] Add to Column 2 (Hadoop)
  - [ ] Add to Column 3 (Databricks)
  - [ ] Add to Column 5 (Gaps/Notes)

- [ ] **Hadoop Logic Sheet - Row 17, Column 6**
  - [ ] Add "Step 14 DETAIL" at the end (or replace Step 14)

- [ ] **Databricks Logic Sheet - Row 11, Column 6**
  - [ ] Add "Step 17 DETAIL" at the end (or replace Step 17)

---

## 💡 WHY THIS MATTERS

**The Difference:**
- **Hadoop:** Patient with leads from Hospital A and Hospital B → **1 lead selected** (best across both)
- **Databricks:** Patient with leads from Hospital A and Hospital B → **2 leads selected** (one from each hospital)

**Impact:**
- Different lead counts in output
- Different lead selection logic
- Different downstream data volumes

Now clients can find this documented in **all three places**!

