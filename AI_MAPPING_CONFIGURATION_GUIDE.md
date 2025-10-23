# AI-Powered Source-to-Target Mapping Tool - Configuration Guide

## 🎯 Overview

This tool uses Azure OpenAI to intelligently analyze Hadoop and Databricks repositories, generating comprehensive source-to-target field mappings with detailed comparison capabilities.

## 🔧 Prerequisites

### 1. Azure OpenAI Setup
- Azure OpenAI service deployed in your Azure subscription
- API key and endpoint URL
- GPT-4 model access (recommended for best results)

### 2. Python Dependencies
```bash
pip install openai pandas openpyxl
```

### 3. Repository Access
- Hadoop repository (e.g., `app-cdd`)
- Databricks repository (e.g., `CDD`)

## ⚙️ Configuration

### Step 1: Azure OpenAI Credentials

Replace the following in `ai_powered_source_target_mapper.py`:

```python
# Configuration - Replace with your Azure OpenAI credentials
AZURE_OPENAI_KEY = "your-azure-openai-api-key-here"
AZURE_OPENAI_ENDPOINT = "https://your-resource.openai.azure.com/"
```

### Step 2: Repository Paths

Update the repository paths:

```python
# Repository paths - Replace with your actual paths
HADOOP_REPO_PATH = "/path/to/your/hadoop/app-cdd"
DATABRICKS_REPO_PATH = "/path/to/your/databricks/CDD"
```

### Step 3: Repository Names

Set the repository names for identification:

```python
# Repository names
HADOOP_REPO_NAME = "app-cdd"
DATABRICKS_REPO_NAME = "CDD"
```

## 🚀 Usage

### Basic Usage

```python
from ai_powered_source_target_mapper import AIPoweredSourceTargetMapper

# Initialize mapper
mapper = AIPoweredSourceTargetMapper(
    azure_openai_key="your-api-key",
    azure_openai_endpoint="https://your-resource.openai.azure.com/"
)

# Analyze repositories
analysis_results = mapper.analyze_repositories(
    hadoop_repo_path="/path/to/hadoop/app-cdd",
    databricks_repo_path="/path/to/databricks/CDD",
    hadoop_repo_name="app-cdd",
    databricks_repo_name="CDD"
)

# Generate reports
report_files = mapper.generate_all_reports(analysis_results)

# Print summary
mapper.print_analysis_summary(analysis_results)
```

### Command Line Usage

```bash
python ai_powered_source_target_mapper.py
```

## 📊 Output Reports

The tool generates three comprehensive Excel reports:

### 1. Hadoop Source-to-Target Mapping (`AI_MAPPING_HADOOP_SOURCE_TARGET_MAPPING.xlsx`)
- **One sheet per table** with detailed field mappings
- **Summary sheet** with table-level statistics
- **Exact column structure** as requested:
  - id, Partner, Schema, Target Table Name, Target Field Name
  - Target Field Data Type, pk?, contains_pii, Field Type
  - Field Depends On, Processing Order, Pre Processing Rules
  - Source Field Names, Source Dataset Name, Field Definition
  - Example 1, Example 2

### 2. Databricks Source-to-Target Mapping (`AI_MAPPING_DATABRICKS_SOURCE_TARGET_MAPPING.xlsx`)
- Same structure as Hadoop report
- Databricks-specific analysis and mappings

### 3. Comparison Report (`AI_MAPPING_COMPARISON_REPORT.xlsx`)
- **Table Comparison**: Field counts, source table differences
- **Field Differences**: Missing fields in each system
- **Missing Tables**: Tables present in one system but not the other

## 🔍 AI Analysis Capabilities

The Azure OpenAI integration provides:

### Intelligent Code Analysis
- **PySpark/Pig/Hive Script Analysis**: Extracts field mappings and transformations
- **Business Logic Understanding**: Identifies processing rules and dependencies
- **Data Type Detection**: Determines field data types and properties
- **PII Detection**: Identifies potentially sensitive fields
- **Primary Key Identification**: Detects primary key relationships

### Advanced Pattern Recognition
- **Source-to-Target Mapping**: Maps fields between source and target tables
- **Transformation Logic**: Understands data transformation rules
- **Dependency Analysis**: Identifies field dependencies and processing order
- **Cross-Reference Analysis**: Links related fields across tables

## 🎯 Key Features

### Hadoop Analysis
- **Oozie Workflow Parsing**: Extracts execution order and dependencies
- **Multi-Technology Support**: PySpark, Pig, Hive, Shell scripts
- **Table Discovery**: Identifies all source and target tables
- **Field-Level Mapping**: Detailed field-to-field transformations

### Databricks Analysis
- **Pipeline Analysis**: Understands Databricks pipeline structure
- **Notebook Processing**: Analyzes PySpark/SQL notebooks
- **Table Relationships**: Maps data flow between notebooks
- **Field Transformations**: Extracts transformation logic

### Comparison Engine
- **Process Differences**: Identifies where implementations differ
- **Field Mapping Differences**: Shows different field transformations
- **Missing Components**: Highlights missing tables or fields
- **Data Type Changes**: Tracks type conversions between systems

## 🔧 Customization Options

### AI Model Configuration
```python
# Use different model for analysis
self.model = "gpt-4"  # or "gpt-3.5-turbo" for faster/cheaper analysis
```

### Field Mapping Customization
```python
# Customize field mapping attributes
field_mapping = FieldMapping(
    id=str(self.field_id_counter),
    partner="YOUR_PARTNER_NAME",  # Customize partner
    schema="YOUR_SCHEMA",        # Customize schema
    # ... other attributes
)
```

### Report Customization
```python
# Custom output file names
report_files = mapper.generate_all_reports(
    analysis_results, 
    output_prefix="CUSTOM_PREFIX"
)
```

## 🚨 Troubleshooting

### Common Issues

1. **Azure OpenAI API Errors**
   - Verify API key and endpoint
   - Check model availability
   - Ensure sufficient quota

2. **Repository Path Issues**
   - Verify paths exist and are accessible
   - Check file permissions
   - Ensure proper repository structure

3. **Memory Issues with Large Repositories**
   - Process repositories in smaller chunks
   - Use GPT-3.5-turbo for faster processing
   - Increase system memory

### Error Handling

The tool includes comprehensive error handling:
- **AI Analysis Failures**: Falls back to regex-based analysis
- **File Access Issues**: Skips problematic files with warnings
- **Parsing Errors**: Continues processing with error logging

## 📈 Performance Optimization

### For Large Repositories
1. **Batch Processing**: Process files in smaller batches
2. **Model Selection**: Use GPT-3.5-turbo for faster processing
3. **Caching**: Implement result caching for repeated analysis
4. **Parallel Processing**: Analyze multiple files simultaneously

### Cost Optimization
1. **Content Truncation**: Limit code content sent to AI (currently 3000 chars)
2. **Selective Analysis**: Focus on critical files only
3. **Model Selection**: Use appropriate model for complexity level

## 🔒 Security Considerations

### API Key Security
- Store API keys in environment variables
- Use Azure Key Vault for production deployments
- Never commit API keys to version control

### Data Privacy
- The tool processes code locally
- Only code snippets are sent to Azure OpenAI
- No sensitive data is transmitted
- Consider data residency requirements

## 📞 Support

For issues or questions:
1. Check the troubleshooting section
2. Review error logs for specific issues
3. Verify Azure OpenAI service status
4. Ensure repository structure matches expected format

## 🎉 Success Metrics

After successful execution, you should see:
- ✅ All three Excel reports generated
- 📊 Comprehensive analysis summary printed
- 🔍 Detailed field mappings for each table
- 📈 Clear comparison between Hadoop and Databricks implementations

The tool provides exactly what you requested: **comprehensive source-to-target field mapping with intelligent comparison between Hadoop and Databricks implementations**.
