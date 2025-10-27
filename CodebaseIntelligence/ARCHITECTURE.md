# System Architecture

## Overview

The Codebase Intelligence Platform is a multi-layer system designed to analyze codebases from different systems (Ab Initio, Hadoop, Databricks), generate Source-to-Target Mappings (STTM), and identify gaps.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    Output Layer                              │
│  Excel Reports │ JSON │ Logs │ (Future: Web UI, Chatbot)   │
└─────────────────────────────────────────────────────────────┘
                          ▲
┌─────────────────────────────────────────────────────────────┐
│                  Analysis & Generation Layer                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │    STTM      │  │     Gap      │  │   Process    │     │
│  │  Generator   │  │   Analyzer   │  │   Matcher    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                          ▲
┌─────────────────────────────────────────────────────────────┐
│                     Data Model Layer                         │
│  Process │ Component │ ColumnMapping │ Gap │ STTMEntry     │
└─────────────────────────────────────────────────────────────┘
                          ▲
┌─────────────────────────────────────────────────────────────┐
│                     Parser Layer                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   AbInitio   │  │    Hadoop    │  │  Databricks  │     │
│  │    Parser    │  │    Parser    │  │    Parser    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│   MP│DML│Flow       Oozie│Spark│Pig    Notebooks│ADF      │
└─────────────────────────────────────────────────────────────┘
                          ▲
┌─────────────────────────────────────────────────────────────┐
│                     Input Layer                              │
│  Ab Initio .mp │ Hadoop Repos │ Databricks Notebooks        │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Parser Layer

#### Ab Initio Parser
- **MPFileParser**: Parses .mp files (graph definitions)
- **DMLParser**: Extracts schema from DML definitions
- **GraphFlowExtractor**: Builds data lineage (NEW - generates GraphFlow)

**Features:**
- Extracts components (Input/Output Files, Transforms, Joins, etc.)
- Parses DML schemas to field level
- Identifies graph parameters
- **NEW**: Generates GraphFlow sheet (source -> target data flows)
- Exports to Excel in FAWN-compatible format

#### Hadoop Parser
- **OozieParser**: Parses workflow.xml and coordinator.xml
- **SparkParser**: Analyzes PySpark Python files
- **PigParser**: Extracts logic from Pig Latin scripts
- **HiveParser**: Parses HiveQL DDL/DML

**Features:**
- Supports Oozie, Spark, Pig, Hive, Shell scripts
- Extracts input/output datasets and tables
- Identifies transformations and business logic
- Handles complex directory structures

#### Databricks Parser (Future)
- Notebook parser
- ADF pipeline parser
- Delta Lake metadata

### 2. Data Model Layer

**Unified data models across all systems:**

**Process**
- Represents workflow/graph/pipeline
- Contains metadata, parameters, schedule
- Links to components

**Component**
- Individual processing unit (job, script, transform)
- Has input/output datasets, schemas
- Contains transformation logic, business description

**ColumnMapping**
- Source-to-target field mapping
- Includes datatype, transformation rules
- Supports PK/FK identification, nullability
- **Critical for STTM reports**

**Gap**
- Represents identified discrepancy
- Types: missing process, logic difference, data coverage, etc.
- Severity: critical, high, medium, low
- Includes recommendations

### 3. Analysis & Generation Layer

#### STTM Generator
Generates column-level Source-to-Target Mappings:

**Capabilities:**
- Process-level STTM (within single system)
- Cross-system STTM (Ab Initio -> Hadoop -> Databricks)
- Schema matching and inference
- Transformation rule extraction
- Processing order calculation

**Output Fields:**
- Source: table, column, datatype, description, PK?, nullable?
- Target: table, column, datatype, description, PK?, nullable?
- Transformation rule, business rule, data quality rule
- Mapping type (direct, derived, lookup, calculated)
- Confidence score

#### Process Matcher
Matches processes across systems using:

1. **Name Similarity** (20%): String matching, token overlap
2. **Table Overlap** (40%): Jaccard similarity of datasets/tables
3. **Business Function** (20%): Keyword matching
4. **Component Similarity** (20%): Type distribution comparison

**Threshold**: 0.7 (configurable)

#### Gap Analyzer
Identifies gaps between systems:

**Gap Types:**
1. **Missing Process**: Exists in source, not in target
2. **Logic Difference**: Different component counts/types
3. **Data Coverage**: Missing tables/datasets
4. **Aggregation Level**: Patient account vs Person ID level
5. **Missing Columns**: Schema differences
6. **Transformation Difference**: Different processing logic
7. **Business Rule**: Different filter/validation rules

**Severity Levels:**
- Critical: Missing core functionality
- High: Significant logic differences
- Medium: Minor differences
- Low: Cosmetic differences

### 4. Output Layer

#### Excel Exporter
Professional Excel reports with:

**STTM Reports:**
- Summary sheet with statistics
- Source_To_Target_Mapping sheet (main STTM)
- Statistics sheet
- Color-coded headers
- Auto-sized columns
- Frozen headers

**Gap Analysis Reports:**
- Gap Summary (counts by type/severity)
- All Gaps detail sheet
- Critical Gaps sheet
- Individual sheets per gap type
- Color-coded by severity
- Executive summary

**Combined Reports:**
- Executive summary
- STTM data
- Gap analysis
- Recommendations

## Data Flow

### Full Analysis Flow:

```
1. Parse Ab Initio files
   └─> Extract processes, components, DML schemas, graph flows
   └─> Export to Excel (FAWN format)

2. Parse Hadoop repository
   └─> Extract workflows, Spark jobs, Pig scripts, Hive DDL
   └─> Identify input/output datasets and tables

3. Generate STTM for each process
   └─> Match input/output schemas
   └─> Extract transformation rules
   └─> Calculate processing order
   └─> Export individual STTM reports

4. Match processes across systems
   └─> Calculate similarity scores
   └─> Identify equivalent process pairs

5. Perform gap analysis
   └─> Find missing processes
   └─> Compare logic and data coverage
   └─> Identify schema differences
   └─> Generate recommendations

6. Generate reports
   └─> Cross-system STTM
   └─> Gap analysis report
   └─> Combined executive report
```

## Configuration

### config/config.yaml

```yaml
parsing:
  abinitio:
    extract_graph_flow: true  # NEW feature
    extract_dml: true
    extract_transformations: true

matching:
  algorithms:
    - name: "table_name_match"
      weight: 0.4
    - name: "semantic_similarity"
      weight: 0.3
    ...
  overall_threshold: 0.7

gap_analysis:
  gap_types: [...]
  priority_rules: {...}

sttm:
  include_fields: [...]
  generation_rules: {...}
```

## Extensibility

### Adding New Parsers

1. Create parser in `parsers/<system>/`
2. Implement `parse_file()` or `parse_directory()`
3. Return `Process` and `Component` objects
4. Register in main runner

### Adding New Gap Types

1. Add enum value to `GapType`
2. Implement detection logic in `GapAnalyzer`
3. Update priority rules in config

### Adding New STTM Fields

1. Add fields to `ColumnMapping` model
2. Update `STTMEntry.to_dict()`
3. Update Excel column headers

## Performance Considerations

### Memory
- Streaming file parsing for large files
- Batch processing for multiple repos
- Lazy loading of schemas

### Speed
- Parallel file parsing (future)
- Caching of parsed results
- Incremental analysis (future)

## Security & Access

- No data storage (code analysis only)
- Read-only file access
- No external API calls (except Azure OpenAI for future RAG)
- All processing local

## Future Enhancements

### Phase 2: RAG & Chatbot
- Azure AI Search integration
- Vector embeddings for semantic search
- LangChain-based chatbot
- Natural language queries

### Phase 3: Web UI
- Interactive dashboard
- Visual lineage diagrams
- Real-time gap tracking
- Collaborative annotations

### Phase 4: CI/CD Integration
- Automated gap detection on commits
- Regression testing
- Version comparison
- Change impact analysis

## Technology Stack

**Languages**: Python 3.9+

**Core Libraries**:
- pandas, numpy: Data processing
- openpyxl: Excel generation
- lxml, xmltodict: XML parsing
- ast: Python code analysis

**Future**:
- langchain: LLM orchestration
- azure-ai-search: Vector search
- fastapi: REST API
- streamlit: Web UI

## Deployment

### Local
```bash
python3 run_analysis.py --abinitio-path ... --hadoop-path ...
```

### Ubuntu VM
```bash
python3 run_analysis.py ...
# Works identically to local
```

### Docker (Future)
```bash
docker run -v /data:/data codebase-intelligence --abinitio-path /data/abi
```

### Azure (Future)
- Azure Functions for serverless
- Azure Batch for large-scale parsing
- Azure Storage for results

## Monitoring & Logging

- Structured logging with loguru
- Log levels: DEBUG, INFO, WARNING, ERROR
- Output to console and file
- Per-module logging configuration

## Known Limitations

1. **Databricks Parser**: Not yet implemented (planned)
2. **Binary .mp Files**: Limited parsing (use FAWN as fallback)
3. **Complex Transformations**: May require manual review
4. **Schema Inference**: Heuristic-based, not 100% accurate
5. **No Runtime Data**: Code analysis only, no actual data

## Support & Troubleshooting

See QUICKSTART.md and README.md for detailed instructions.
