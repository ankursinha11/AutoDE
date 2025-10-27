# Codebase Intelligence Platform

Healthcare Finance Data Pipeline Gap Analysis & STTM Generation System

## Overview

This platform analyzes code from Ab Initio, Hadoop, and Azure Databricks to:
- Identify gaps in logic and data coverage
- Generate Source-to-Target Mappings (STTM) at column level
- Provide AI-powered gap analysis via RAG chatbot
- Create comprehensive Excel reports

## Project Structure

```
CodebaseIntelligence/
├── parsers/                  # Code parsers for different systems
│   ├── abinitio/            # Ab Initio .mp file parser (enhanced FAWN)
│   ├── hadoop/              # Hadoop Oozie, Spark, Pig, Hive parsers
│   └── databricks/          # Databricks notebook & ADF parser
├── core/                    # Core business logic
│   ├── models/              # Data models
│   ├── matchers/            # Cross-system matching algorithms
│   ├── gap_analyzer/        # Gap detection engine
│   └── sttm_generator/      # Source-to-Target mapping generator
├── services/                # External services integration
│   ├── azure_search/        # Azure AI Search (RAG)
│   ├── openai/              # Azure OpenAI integration
│   └── database/            # Metadata database
├── utils/                   # Utility functions
├── api/                     # REST API (FastAPI)
├── outputs/                 # Generated reports and logs
├── tests/                   # Unit and integration tests
└── config/                  # Configuration files
```

## Key Features

### 1. Multi-System Parsing
- **Ab Initio**: .mp files, .ksh scripts, DML schemas, GraphFlow extraction
- **Hadoop**: Oozie workflows, PySpark jobs, Pig scripts, Hive DDL/DML
- **Databricks**: Notebooks, ADF pipelines, Delta tables

### 2. Source-to-Target Mapping (STTM)
Column-level mappings with:
- Source name, datatype, description
- Target name, datatype, description
- Primary key indicators
- Processing order
- Transformation rules
- Data quality rules

### 3. Gap Analysis
- Missing processes
- Logic differences
- Data coverage gaps
- Aggregation level mismatches
- Business rule variations

### 4. AI-Powered Intelligence
- RAG chatbot for code queries
- Semantic similarity matching
- Automated gap detection
- Natural language reports

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure Azure services in `config/azure_config.yaml`

3. Run parsers:
```bash
python -m parsers.abinitio.parser --input /path/to/abinitio/files
python -m parsers.hadoop.parser --input /path/to/hadoop/repo
python -m parsers.databricks.parser --workspace-url <url>
```

4. Generate STTM and gap analysis:
```bash
python -m core.sttm_generator.generator
python -m core.gap_analyzer.analyzer
```

5. Start API server:
```bash
python -m api.main
```

## Usage

See individual module READMEs for detailed usage instructions.

## Client: Finthrive
Healthcare finance management - lead generation pipeline analysis
