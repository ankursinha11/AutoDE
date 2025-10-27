# Quick Start Guide

## For Ubuntu VM Environment

This tool is fully compatible with Ubuntu and Python 3.9+.

## Installation

### 1. Clone or Copy the Project

```bash
# If in your VM
cd /path/to/your/workspace
cp -r CodebaseIntelligence ./
cd CodebaseIntelligence
```

### 2. Install Python Dependencies

```bash
# Install Python 3.9+ if not already installed
sudo apt update
sudo apt install python3 python3-pip -y

# Install project dependencies
pip3 install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
# Copy example env file
cp .env.example .env

# Edit with your Azure credentials
nano .env
```

Update these values in `.env`:
```
AZURE_OPENAI_ENDPOINT=https://your-instance.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
AZURE_SEARCH_ENDPOINT=https://your-search.search.windows.net
AZURE_SEARCH_API_KEY=your-search-key
```

## Quick Test

Test the system with your existing data:

```bash
python3 test_system.py
```

This will:
- Parse your Ab Initio sample files
- Parse your Hadoop repository
- Generate STTM reports
- Perform gap analysis
- Export Excel reports to `./outputs/test_reports/`

## Full Analysis

Run complete analysis:

```bash
python3 run_analysis.py \
  --abinitio-path /path/to/abinitio/files \
  --hadoop-path /path/to/hadoop/repo \
  --mode full \
  --output-dir ./outputs/reports
```

### Options:

- `--mode parse`: Only parse codebases
- `--mode sttm`: Parse and generate STTM
- `--mode gap`: Parse and perform gap analysis
- `--mode full`: Complete analysis (default)

## Output Files

All reports are saved to `outputs/reports/`:

### STTM Reports:
- `STTM_<ProcessName>.xlsx` - Individual process STTM
- `STTM_Cross_System.xlsx` - Cross-system mappings

### Gap Analysis Reports:
- `Gap_Analysis_Report.xlsx` - Detailed gap analysis with:
  - Gap Summary sheet
  - All Gaps sheet
  - Critical Gaps sheet
  - Gaps by Type sheets

### Combined Report:
- `Combined_Analysis_Report.xlsx` - Executive summary with both STTM and gaps

## Excel Report Structure

### STTM Report Sheets:

**1. Summary**
- Total mappings
- Mapping type breakdown
- Source/target systems
- Table counts

**2. Source_To_Target_Mapping**
Columns:
- STTM_ID
- Process_Name
- Source_Table, Source_Column, Source_Datatype
- Target_Table, Target_Column, Target_Datatype
- Source_Is_PK, Target_Is_PK
- Processing_Order
- Transformation_Rule
- Business_Rule
- Data_Quality_Rule
- Mapping_Confidence

**3. Statistics**
- Mapping type distribution
- Table coverage

### Gap Analysis Report Sheets:

**1. Gap Summary**
- Total gaps by type
- Total gaps by severity (color-coded)

**2. All Gaps / Critical Gaps**
Columns:
- Gap ID
- Type, Severity
- Source/Target System
- Source/Target Process
- Description
- Business Impact
- Recommendation
- Confidence

## Troubleshooting

### Import Errors
If you get import errors, ensure you're running from the project root:
```bash
cd /path/to/CodebaseIntelligence
python3 test_system.py
```

### Missing Dependencies
```bash
pip3 install --upgrade -r requirements.txt
```

### Permission Errors
```bash
chmod +x run_analysis.py test_system.py
```

### Output Directory
The tool auto-creates output directories, but you can manually create:
```bash
mkdir -p outputs/reports outputs/logs
```

## Next Steps

1. **Run Test**: `python3 test_system.py`
2. **Review Reports**: Check `outputs/test_reports/`
3. **Run Full Analysis**: Use `run_analysis.py` with your paths
4. **Customize**: Edit `config/config.yaml` for custom settings

## For Production Use

### Azure AI Search Integration (Optional)
For RAG chatbot functionality, setup Azure AI Search:

```python
# Will be implemented in Phase 2
# Currently generates static Excel reports
```

### Process More Repositories
Update paths in `run_analysis.py` or pass multiple directories:

```bash
# Process multiple Hadoop repos
python3 run_analysis.py \
  --abinitio-path /path/to/abi \
  --hadoop-path /path/to/hadoop/repo1 \
  --output-dir ./outputs/repo1_analysis

python3 run_analysis.py \
  --abinitio-path /path/to/abi \
  --hadoop-path /path/to/hadoop/repo2 \
  --output-dir ./outputs/repo2_analysis
```

## Support

For issues or questions:
1. Check logs in `outputs/logs/app.log`
2. Run with debug logging: `--log-level DEBUG`
3. Review code comments and docstrings

## Ubuntu-Specific Notes

### Installing Python 3.9+ on Ubuntu
```bash
sudo apt update
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt install python3.9 python3.9-pip -y
```

### Setting Python 3.9 as Default
```bash
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1
```

### Virtual Environment (Recommended)
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

All tools are cross-platform and work identically on Ubuntu, Windows, and macOS!
