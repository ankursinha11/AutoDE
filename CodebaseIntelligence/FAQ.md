# Frequently Asked Questions

## General Questions

### Q1: Why is the API folder empty?

**A:** The API folder is for **future Phase 2** (RAG Chatbot & Web Interface).

**Current Phase 1** is focused on:
- Code parsing
- STTM generation
- Gap analysis
- Excel report generation

You don't need the API right now. Everything works via command-line scripts.

**Future API will provide:**
- REST endpoints for web dashboard
- Real-time query interface
- Integration with other tools

---

### Q2: Does this work on Ubuntu without a GUI?

**A:** **Yes, absolutely!** Ubuntu Server (no GUI) works perfectly.

**How it works:**
1. Connect via SSH (command line)
2. Run: `python3 run_analysis.py ...`
3. Tool generates Excel files in `outputs/reports/`
4. Copy Excel files back to Windows VDI
5. Open Excel files in Windows

**You never need to see/open files on Ubuntu.** The tool just generates them.

---

### Q3: Can I use FAWN instead of your Ab Initio parser?

**A:** **Yes! Use both (recommended hybrid approach):**

| Tool | What It Does | When to Use |
|------|-------------|-------------|
| **FAWN** | Parse Ab Initio .mp files | More mature, handles complex .mp files better |
| **Our Tool** | GraphFlow + Hadoop + STTM + Gap Analysis | Integrated analysis across systems |

**Best Approach: Hybrid**
```bash
# 1. Use FAWN for Ab Initio parsing
FAWN.exe generate-excel 400_commGenIpa.mp

# 2. Import FAWN output into our tool
python3 run_analysis.py \
  --fawn-excel 400_commGenIpa_FAWN_output.xlsx \
  --hadoop-path /path/to/hadoop \
  --mode full
```

Our tool can **import FAWN Excel output** and use it!

---

### Q4: How confident are you in the Ab Initio parser?

**Honest Answer:**

✅ **Confident for:**
- Basic .mp files
- Component extraction
- DML parsing
- GraphFlow generation (FAWN doesn't do this!)

⚠️ **Less confident for:**
- Complex binary .mp files
- Advanced Ab Initio features
- Edge cases FAWN handles better

**Recommendation:** Use FAWN for parsing, our tool for everything else.

---

## Deployment Questions

### Q5: How do I deploy to Azure VM?

**Complete flow:**

```
Your Mac → Client VDI → Azure Ubuntu VM

1. Package on Mac:
   tar -czf CodebaseIntelligence.tar.gz CodebaseIntelligence/

2. Transfer to VDI (via file share/SCP)

3. From VDI to VM:
   scp CodebaseIntelligence.tar.gz azureuser@vm-ip:/home/azureuser/

4. On VM:
   tar -xzf CodebaseIntelligence.tar.gz
   cd CodebaseIntelligence
   ./INSTALL.sh

5. Run analysis:
   python3 run_analysis.py --abinitio-path /mnt/share/abi --hadoop-path /mnt/share/hadoop

6. Copy results back to VDI:
   scp -r outputs/reports/ user@vdi:/path/to/local
```

See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for details.

---

### Q6: What if my Ubuntu VM doesn't have internet access?

**A:** No problem! You can install dependencies offline:

**On a machine with internet:**
```bash
pip3 download -r requirements.txt -d packages/
tar -czf packages.tar.gz packages/
```

**Transfer packages.tar.gz to VM, then:**
```bash
tar -xzf packages.tar.gz
pip3 install --no-index --find-links=packages/ -r requirements.txt
```

---

### Q7: How do I mount Azure File Share on Ubuntu VM?

```bash
# Create mount point
sudo mkdir -p /mnt/datashare

# Mount with CIFS
sudo mount -t cifs \
  //<storage-account>.file.core.windows.net/<share-name> \
  /mnt/datashare \
  -o username=<storage-account>,password=<access-key>,vers=3.0,uid=$(id -u),gid=$(id -g)

# Verify
ls /mnt/datashare

# Auto-mount on reboot (add to /etc/fstab)
echo "//<storage>.file.core.windows.net/<share> /mnt/datashare cifs username=<account>,password=<key>,vers=3.0,uid=$(id -u),gid=$(id -g) 0 0" | sudo tee -a /etc/fstab
```

---

## Usage Questions

### Q8: How long does analysis take?

**Depends on repo size:**

| Repo Size | Parsing | STTM | Gap Analysis | Total |
|-----------|---------|------|--------------|-------|
| Small (10-50 files) | 30s | 1min | 30s | ~2min |
| Medium (50-200 files) | 2min | 5min | 2min | ~10min |
| Large (200-500 files) | 5min | 15min | 5min | ~25min |
| Very Large (500+ files) | 15min | 30min | 10min | ~1hr |

**Your current repos:** Likely 5-15 minutes per repo.

---

### Q9: What outputs do I get?

**Excel Reports:**

1. **STTM_ProcessName.xlsx** (one per process)
   - Source-to-Target mappings at column level
   - Transformation rules
   - All metadata fields

2. **STTM_Cross_System.xlsx**
   - Cross-system mappings (Abi → Hadoop → Databricks)

3. **Gap_Analysis_Report.xlsx**
   - Summary by type & severity
   - Detailed gap listings
   - Recommendations
   - Color-coded

4. **Combined_Analysis_Report.xlsx**
   - Executive summary
   - All-in-one report

**Also generated:**
- `AbInitio_Parsed_Output.xlsx` - FAWN-like format
- Logs in `outputs/logs/app.log`

---

### Q10: Can I process multiple Hadoop repos?

**Yes! Two approaches:**

**Approach 1: One at a time**
```bash
python3 run_analysis.py --hadoop-path /path/to/repo1 --output-dir outputs/repo1
python3 run_analysis.py --hadoop-path /path/to/repo2 --output-dir outputs/repo2
python3 run_analysis.py --hadoop-path /path/to/repo3 --output-dir outputs/repo3
```

**Approach 2: Combined**
```bash
# Parse all in one directory
python3 run_analysis.py --hadoop-path /mnt/datashare/all-hadoop-repos --mode full
```

---

### Q11: How do I customize matching thresholds?

Edit `config/config.yaml`:

```yaml
matching:
  algorithms:
    - name: "table_name_match"
      weight: 0.4      # Increase for more table-based matching
    - name: "semantic_similarity"
      weight: 0.3      # Increase for name-based matching
  overall_threshold: 0.7  # Lower = more matches (less strict)
                         # Higher = fewer matches (more strict)
```

Then run:
```bash
python3 run_analysis.py ... --config config/config.yaml
```

---

### Q12: Can I run this in parallel for multiple repos?

**Yes! Use GNU parallel or screen sessions:**

```bash
# Option 1: GNU parallel
parallel python3 run_analysis.py --hadoop-path {} --output-dir outputs/{/} ::: /path/to/repos/*

# Option 2: Multiple screen sessions
screen -S repo1
python3 run_analysis.py --hadoop-path /path/repo1 ...
# Ctrl+A, D to detach

screen -S repo2
python3 run_analysis.py --hadoop-path /path/repo2 ...
# Ctrl+A, D to detach

# Check progress
screen -ls
screen -r repo1  # Reattach to see progress
```

---

## Technical Questions

### Q13: What Python version do I need?

**Minimum: Python 3.9**

**Check version:**
```bash
python3 --version
```

**If < 3.9, install on Ubuntu:**
```bash
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.9 python3.9-pip -y
```

---

### Q14: Do I need Azure OpenAI for basic analysis?

**No!** Azure OpenAI is only needed for **Phase 2 (RAG Chatbot)**.

**Current Phase 1 works WITHOUT:**
- Azure OpenAI
- Azure AI Search
- Any Azure services

**Only uses:**
- Python
- File system access
- Excel generation (openpyxl)

---

### Q15: Can I add custom gap detection rules?

**Yes!** Edit `core/gap_analyzer/analyzer.py`:

```python
def _check_custom_business_rule(self, source_proc, target_proc):
    """Add your custom gap detection"""
    # Example: Check for specific table presence
    if "patient_demographics" in source_proc.tables_involved:
        if "patient_demographics" not in target_proc.tables_involved:
            self.gaps.append(Gap(
                gap_type=GapType.BUSINESS_RULE,
                severity=GapSeverity.HIGH,
                title="Missing Patient Demographics Table",
                description="Source has patient_demographics but target doesn't",
                ...
            ))
```

---

### Q16: How do I add support for Databricks?

**Structure is already in place!** Just implement the parser:

```python
# parsers/databricks/parser.py
class DatabricksParser:
    def parse_notebook(self, notebook_path):
        # Parse .py, .sql, .scala notebooks
        # Extract cells, queries, transformations
        # Return Component objects
        pass

    def parse_adf_pipeline(self, pipeline_json):
        # Parse ADF pipeline JSON
        # Extract activities, dependencies
        # Return Process object
        pass
```

Then use it:
```bash
python3 run_analysis.py \
  --abinitio-path /path/to/abi \
  --hadoop-path /path/to/hadoop \
  --databricks-path /path/to/databricks \
  --mode full
```

---

## Troubleshooting

### Q17: I get "ModuleNotFoundError"

**Solution:**
```bash
# Ensure running from project root
cd CodebaseIntelligence
python3 -m pip install --user -r requirements.txt

# Or use full path
python3 /home/azureuser/CodebaseIntelligence/run_analysis.py ...
```

---

### Q18: Excel files are corrupted or won't open

**Solution:**
- Check file size: `ls -lh outputs/reports/*.xlsx`
- If 0 bytes, check logs: `cat outputs/logs/app.log`
- Try with debug: `python3 run_analysis.py ... --log-level DEBUG`

---

### Q19: Out of memory on large repos

**Solution:**
```bash
# Process in smaller batches
python3 run_analysis.py --mode parse  # Just parsing first

# Or increase VM memory
# Azure Portal → VM → Size → Select larger size
```

---

### Q20: How do I get help?

1. **Check logs:** `outputs/logs/app.log`
2. **Run with debug:** `--log-level DEBUG`
3. **Read docs:**
   - `README.md` - Overview
   - `QUICKSTART.md` - Quick start
   - `ARCHITECTURE.md` - Technical details
   - `DEPLOYMENT_GUIDE.md` - Deployment
4. **Check code comments** - Extensive inline documentation

---

## Best Practices

### Q21: What's the recommended workflow?

**Production Workflow:**

```bash
# 1. Test locally first
python3 test_system.py

# 2. Run on sample data
python3 run_analysis.py --abinitio-path samples/abi --hadoop-path samples/hadoop

# 3. Validate sample reports

# 4. Deploy to VM

# 5. Run on full repos (one at a time initially)
python3 run_analysis.py --abinitio-path /mnt/share/abi --mode sttm

# 6. Review STTM reports

# 7. Run gap analysis
python3 run_analysis.py --abinitio-path /mnt/share/abi --hadoop-path /mnt/share/hadoop --mode gap

# 8. Share reports with team

# 9. Iterate based on feedback
```

---

### Q22: Should I use FAWN or built-in parser?

**Decision Matrix:**

| Scenario | Use FAWN | Use Built-in | Use Both |
|----------|----------|--------------|----------|
| Simple .mp files | ✓ | ✓ | ✓ |
| Complex .mp files | ✓✓ | ⚠️ | ✓✓ |
| Need GraphFlow | | ✓✓ | ✓✓ |
| Hadoop integration | | ✓✓ | ✓✓ |
| STTM generation | | ✓✓ | ✓✓ |
| Gap analysis | | ✓✓ | ✓✓ |

**Recommendation: Use both!**
- FAWN for reliable Ab Initio parsing
- Our tool for GraphFlow, STTM, gaps, and Hadoop

---

Still have questions? Check the code - it has extensive comments!
