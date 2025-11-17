"""
STAG STTM (Source-to-Target Mapping) Generator

Generates comprehensive column-level mappings between source and target systems
using advanced AI analysis and RAG techniques.

Key Features:
- Schema inference from workflow logic
- Intelligent column matching (semantic, not just name-based)
- Transformation logic generation
- Dependency identification
- Confidence scoring
- Multi-system support (Hadoop, Ab Initio, Databricks)

Optimized for best RAG results with advanced prompt engineering.
"""

from typing import Dict, List, Any, Optional
from loguru import logger
import json
import re
import traceback


class STAGSTTMGenerator:
    """Generate Source-to-Target Mappings using advanced AI analysis"""

    def __init__(self, ai_analyzer=None, indexer=None):
        """
        Initialize STAG STTM Generator

        Args:
            ai_analyzer: AIAnalyzer for schema inference and mapping
            indexer: MultiCollectionIndexer for schema search
        """
        self.ai_analyzer = ai_analyzer
        self.indexer = indexer

    def _search_schema_files(self, workflow_name: str, system: str) -> List[Dict]:
        """
        Search for schema definition files in vector DB

        For Ab Initio: Use FAWN/VM outputs instead of raw .mp files
        For Hadoop: Search for CREATE TABLE, DDL
        For Databricks: Search for DataFrame schemas
        """
        if not self.indexer:
            logger.warning("   No indexer for schema search")
            return []

        try:
            # Build search query based on system
            if system == 'abinitio':
                # Search FAWN outputs and STTM collections
                query = f"{workflow_name} DML XFR record_format schema column"
                collections = ["abinitio_collection", "abinitio_sttm_collection"]
            elif system == 'hadoop':
                query = f"{workflow_name} CREATE TABLE schema DDL Hive columns"
                collections = ["hadoop_collection"]
            elif system == 'databricks':
                query = f"{workflow_name} StructType schema createDataFrame columns"
                collections = ["databricks_collection"]
            else:
                return []

            search_results = self.indexer.search_multi_collection(
                query=query,
                collections=collections,
                top_k=30  # Get comprehensive schema docs
            )

            schema_docs = []
            for collection in collections:
                docs = search_results.get(collection, [])
                schema_docs.extend(docs)

            logger.info(f"   📋 Found {len(schema_docs)} schema documents for {system}")
            return schema_docs

        except Exception as e:
            logger.error(f"   Schema file search failed: {e}")
            return []

    def generate_sttm(
        self,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any],
        source_system: str
    ) -> List[Dict[str, Any]]:
        """
        Generate Source-to-Target Mappings using CODE-EXTRACTED schemas as grounding truth

        NEW APPROACH:
        - Extract schemas directly from pre-parsed column_schemas (NO hallucinations!)
        - Use AI ONLY for semantic matching, transformation descriptions, and comparison notes

        Args:
            source_logic: Logic from Hadoop or Ab Initio (with column_schemas from code extraction)
            databricks_logic: Logic from Databricks (with column_schemas from code extraction)
            source_system: "hadoop" or "abinitio"

        Returns:
            [
                {
                    'target_column': str,
                    'data_type': str,
                    'source_columns': List[str],
                    'transformation_logic': str,
                    'dependencies': List[str],
                    'confidence': float
                }
            ]
        """
        logger.info(f"🔗 Generating STTM from {source_system} to Databricks (using code-extracted schemas)")

        try:
            # Step 1: Extract schemas from CODE (NOT RAG!) - NO HALLUCINATIONS
            source_schema = self._extract_schema_from_code(source_logic, source_system)
            target_schema = self._extract_schema_from_code(databricks_logic, 'databricks')

            # Handle None schemas before trying to access them
            if not source_schema or not target_schema:
                source_col_count = len(source_schema.get('columns', [])) if source_schema else 0
                target_col_count = len(target_schema.get('columns', [])) if target_schema else 0
                logger.warning(f"   ⚠ Code-extracted schemas incomplete: source={source_col_count} cols, target={target_col_count} cols")

                # Still try to generate mappings with whatever we have
                if not source_schema:
                    source_schema = {'columns': [], 'table_name': 'Unknown', 'schema': source_system}
                if not target_schema:
                    target_schema = {'columns': [], 'table_name': 'Unknown', 'schema': 'databricks'}

            logger.info(f"   📋 Source schema: {len(source_schema.get('columns', []))} columns")
            logger.info(f"   📋 Target schema: {len(target_schema.get('columns', []))} columns")

            # Step 2: Generate column mappings using AI (grounded in extracted schemas)
            if self.ai_analyzer:
                mappings = self._generate_column_mappings_with_ai(
                    source_schema,
                    target_schema,
                    source_logic,
                    databricks_logic,
                    source_system
                )
            else:
                # Fallback: simple name-based matching
                mappings = self._generate_simple_mappings(source_schema, target_schema)

            logger.info(f"✅ Generated {len(mappings)} column mappings")
            return mappings

        except Exception as e:
            logger.error(f"STTM generation failed: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _extract_schema_from_code(
        self,
        logic: Dict[str, Any],
        system: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract schema directly from CODE-EXTRACTED column_schemas (NO AI, NO RAG, NO HALLUCINATIONS!)

        This is the NEW grounding truth approach that uses the already-parsed column_schemas
        from databricks_logic_extractor.py and hadoop_logic_extractor.py

        Args:
            logic: Logic dict containing 'activities' (Databricks) or 'jobs' (Hadoop) with column_schemas
            system: "databricks", "hadoop", or "abinitio"

        Returns:
            {
                'table_name': str,
                'schema': str,
                'columns': [
                    {
                        'name': str,
                        'type': str,
                        'order': int,
                        'transformation': str,
                        'source_line': int
                    }
                ],
                'source_files': List[str]
            }
        """
        logger.info(f"   📋 Extracting schema from CODE for {system}")

        all_columns = []
        source_files = []
        consolidated_table_name = "consolidated_output"

        if system == 'databricks':
            # Extract from activities
            activities = logic.get('activities', [])
            for activity in activities:
                activity_name = activity.get('name', 'Unknown')
                notebook_path = activity.get('notebook', '')
                column_schemas = activity.get('column_schemas', {})

                if notebook_path:
                    source_files.append(notebook_path)

                # Consolidate all columns from all tables in this activity
                for table_name, columns in column_schemas.items():
                    for col in columns:
                        # Add table context to avoid duplicates from different tables
                        col_copy = col.copy()
                        col_copy['source_table'] = table_name
                        col_copy['source_activity'] = activity_name
                        all_columns.append(col_copy)

        elif system in ['hadoop', 'abinitio']:
            # Extract from jobs
            jobs = logic.get('jobs', [])
            for job in jobs:
                script_name = job.get('script_file', job.get('name', 'Unknown'))
                column_schemas = job.get('column_schemas', {})

                if script_name:
                    source_files.append(script_name)

                # Consolidate all columns from all tables in this job
                for table_name, columns in column_schemas.items():
                    for col in columns:
                        col_copy = col.copy()
                        col_copy['source_table'] = table_name
                        col_copy['source_script'] = script_name
                        all_columns.append(col_copy)

        # Deduplicate columns by name (keep first occurrence)
        seen_names = set()
        unique_columns = []
        for col in all_columns:
            col_name = col.get('name', '')
            if col_name and col_name not in seen_names:
                seen_names.add(col_name)
                unique_columns.append(col)

        logger.info(f"      ✅ Extracted {len(unique_columns)} unique columns from {len(source_files)} source files")

        if not unique_columns:
            logger.warning(f"      ⚠ No columns extracted from {system} - column_schemas may be empty!")
            return None

        # Infer schema name based on table names and script names
        schema_name = self._infer_schema_name(unique_columns, source_files, system)

        return {
            'table_name': consolidated_table_name,
            'schema': schema_name,
            'columns': unique_columns,
            'source_files': source_files
        }

    def _infer_schema_name(self, columns: List[Dict], source_files: List[str], system: str) -> str:
        """
        Infer business schema name from table names and source files

        Examples:
        - Hadoop tables like 'permIdPatientAcctId', 'allDistinctRecs' -> ES_BDF
        - Hadoop tables with 'swift' in path -> ES_SWIFT
        - Hadoop tables with 'permid' in name -> PERMID_DATA
        - Databricks -> DATABRICKS_BDF (or based on output names)
        """
        # Check table names from columns
        table_names = set()
        for col in columns:
            if 'source_table' in col:
                table_names.add(col['source_table'].lower())

        # Check source files
        source_paths = ' '.join(source_files).lower()

        # Hadoop schema inference
        if system in ['hadoop', 'abinitio']:
            # Check for SWIFT references
            if 'swift' in source_paths or any('swift' in t for t in table_names):
                return 'ES_SWIFT'

            # Check for PERMID references
            if 'permid' in source_paths or any('permid' in t for t in table_names):
                return 'PERMID_DATA'

            # Default for Entity Search BDF
            if 'bdf' in source_paths or any('bdf' in t for t in table_names):
                return 'ES_BDF'

            # Fallback
            return 'HADOOP_DATA'

        elif system == 'databricks':
            # Check for SWIFT references
            if 'swift' in source_paths or any('swift' in t for t in table_names):
                return 'DATABRICKS_SWIFT'

            # Check for PERMID references
            if 'permid' in source_paths or any('permid' in t for t in table_names):
                return 'PERMID_DATA'

            # Default for Databricks BDF
            return 'DATABRICKS_BDF'

        # Fallback
        return system.upper()

    def _extract_schema_with_rag(
        self,
        logic: Dict[str, Any],
        system: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract schema using RAG with ENHANCED schema-specific prompting

        Returns:
            {
                'table_name': str,
                'schema': str,
                'columns': [
                    {
                        'name': str,
                        'type': str,
                        'description': str,
                        'field_type': str,  # NEW: Identifier, Demographic, etc.
                        'contains_pii': bool,  # NEW
                        'is_primary_key': bool,  # NEW
                        'depends_on': List[str],  # NEW
                        'transformation_rule': str  # NEW
                    }
                ],
                'source_files': List[str]
            }
        """
        logger.info(f"   📋 Extracting ENHANCED schema for {system} using RAG")

        # Get workflow name
        workflow_name = str(logic.get('workflow_name') or logic.get('graph_name') or logic.get('pipeline_name', 'Unknown'))

        # CRITICAL: Search for schema files FIRST
        schema_docs = self._search_schema_files(workflow_name, system)

        if schema_docs:
            # Build rich context from schema files
            schema_context = "\n\n".join([
                f"=== FILE: {doc.get('metadata', {}).get('file_name', 'Unknown')} ===\n{doc.get('content', '')[:5000]}"
                for doc in schema_docs[:10]  # First 10 docs
            ])
            logger.info(f"   Using {len(schema_docs)} schema files for context")
        else:
            # Fallback to logic context
            schema_context = self._create_schema_context(logic, system)
            logger.warning(f"   No schema files found - using logic context")

        # Escape curly braces
        workflow_name = workflow_name.replace('{', '{{').replace('}', '}}')
        schema_context = schema_context.replace('{', '{{').replace('}', '}}')

        # ENHANCED RAG prompt for schema extraction with 13-column metadata
        prompt = f"""You are a data schema expert extracting COMPLETE table schemas with ALL column metadata.

# WORKFLOW INFORMATION
System: {system}
Workflow: {workflow_name}

# SCHEMA FILES AND CONTEXT
{schema_context}

# TASK: Extract COMPLETE Output Schema with ENHANCED Metadata

Extract ALL columns from the output schema with comprehensive metadata for each column.

For EACH column, provide:
1. **name**: Column name
2. **type**: Data type (STRING, INTEGER, DECIMAL, DATE, TIMESTAMP, BOOLEAN, etc.)
3. **description**: Business meaning (what this column represents)
4. **field_type**: Classification - choose from:
   - "Identifier" (IDs, keys, unique identifiers)
   - "Demographic" (Name, DOB, Gender, Address, ethnicity)
   - "Financial" (Amount, Balance, Price, revenue)
   - "Calculated" (Derived from other columns)
   - "Reference" (Foreign keys, lookup values)
   - "Status" (Flags, statuses, indicators)
   - "Metadata" (Created date, updated date, version)
5. **contains_pii**: true/false - PII includes: SSN, Name (first/last/full), DOB, Address, Phone, Email, Account numbers
6. **is_primary_key**: true/false
7. **depends_on**: List of source columns this depends on (for calculated fields) - empty array if direct mapping
8. **transformation_rule**: How derived (e.g., "CONCAT(first_name, ' ', last_name)" or "DIRECT" for direct copy)

# OUTPUT FORMAT (JSON):

{{
  "table_name": "output_table_name",
  "schema": "SCHEMA_NAME",
  "columns": [
    {{
      "name": "patient_id",
      "type": "STRING",
      "description": "Unique patient identifier",
      "field_type": "Identifier",
      "contains_pii": false,
      "is_primary_key": true,
      "depends_on": [],
      "transformation_rule": "DIRECT"
    }},
    {{
      "name": "full_name",
      "type": "STRING",
      "description": "Patient full name (first + last)",
      "field_type": "Demographic",
      "contains_pii": true,
      "is_primary_key": false,
      "depends_on": ["first_name", "last_name"],
      "transformation_rule": "CONCAT(first_name, ' ', last_name)"
    }}
  ],
  "source_files": ["schema.dml", "transform.xfr"]
}}

# CRITICAL REQUIREMENTS:
- Extract ALL columns (aim for 50-100+ columns for typical data tables)
- Be EXHAUSTIVE - don't skip columns
- Classify field_type accurately
- Detect PII carefully (SSN, names, DOB, addresses, phone, email)
- Provide actual transformation formulas (not just "transformation")

Now extract the COMPLETE schema:
"""

        try:
            ai_response = self.ai_analyzer.analyze_code(
                code=prompt,
                context=f"Schema extraction for {system}",
                analysis_type="lineage_extraction"
            )
            schema = self._parse_schema_response(ai_response)

            if schema and schema.get('columns'):
                logger.info(f"   ✅ Extracted {len(schema['columns'])} columns for {system}")
                return schema
            else:
                logger.warning(f"   ⚠ No schema extracted for {system}")
                return None

        except Exception as e:
            logger.error(f"Schema extraction failed for {system}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def _create_schema_context(self, logic: Dict[str, Any], system: str) -> str:
        """Create rich context for schema extraction"""
        context_parts = []

        if system == 'hadoop':
            # Include job outputs
            jobs = logic.get('jobs', [])
            if jobs:
                context_parts.append("Job Outputs:")
                for job in jobs:
                    if job.get('outputs'):
                        context_parts.append(f"  - {job['name']}: {', '.join(job['outputs'][:3])}")

        elif system == 'abinitio':
            # Include DML files and component outputs
            dml_files = logic.get('dml_files', [])
            if dml_files:
                context_parts.append(f"DML Files: {', '.join(dml_files[:5])}")

            steps = logic.get('steps', [])
            if steps:
                context_parts.append("\nOutput Components:")
                # Handle both dict and string formats
                output_steps = [s for s in steps if isinstance(s, dict) and 'output' in s.get('component_type', '').lower()]
                for step in output_steps[:3]:
                    if isinstance(step, dict):
                        dataset = step.get('dataset', 'Unknown')
                        transformation = step.get('transformation_rules', '')
                        context_parts.append(f"  - {dataset}: {transformation}")

        elif system == 'databricks':
            # Include notebook outputs and sink information
            activities = logic.get('activities', [])
            if activities:
                context_parts.append("Output Activities:")
                for activity in activities:
                    if activity.get('outputs'):
                        context_parts.append(f"  - {activity['name']}: {', '.join(activity['outputs'][:3])}")

                    # Include code snippets if available
                    if activity.get('code_snippets'):
                        context_parts.append(f"    Code: {activity['code_snippets'][0][:200]}...")

        return '\n'.join(context_parts) if context_parts else "No schema information available"

    def _parse_schema_response(self, ai_response: str) -> Optional[Dict[str, Any]]:
        """Parse AI schema extraction response"""
        try:
            # Extract JSON from response
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r'\{.*"columns".*\}', ai_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    return None

            schema = json.loads(json_str)
            return schema

        except Exception as e:
            logger.warning(f"Failed to parse schema response: {e}")
            return None

    def _generate_column_mappings_with_ai(
        self,
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any],
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any],
        source_system: str
    ) -> List[Dict[str, Any]]:
        """
        Generate column mappings using AI - GROUNDED in code-extracted schemas

        CRITICAL RULES:
        - AI can ONLY match columns from the provided schemas
        - AI CANNOT invent or hallucinate column names
        - AI is used for: semantic matching, transformation description, comparison notes

        Returns:
            List of column mappings for comparison section
        """
        logger.info(f"   🤖 Generating AI-based column mappings (grounded in extracted schemas)")

        source_columns = source_schema.get('columns', [])
        target_columns = target_schema.get('columns', [])

        if not source_columns and not target_columns:
            logger.warning("      ⚠ No columns in either source or target schema!")
            return []

        # Build explicit column lists for AI prompt
        source_col_list = "\n".join([
            f"  - {col.get('name')}: {col.get('type')} (from {col.get('source_table', 'unknown')})"
            for col in source_columns
        ])

        target_col_list = "\n".join([
            f"  - {col.get('name')}: {col.get('type')} (from {col.get('source_table', 'unknown')})"
            for col in target_columns
        ])

        # Create AI prompt with STRICT anti-hallucination instructions
        prompt = f"""You are a data migration expert analyzing column mappings between {source_system.upper()} (source) and DATABRICKS (target).

**CRITICAL RULES - NO EXCEPTIONS:**
1. You can ONLY use columns from the lists below - these are extracted from actual code
2. You are FORBIDDEN from inventing, guessing, or hallucinating any column names
3. If a column doesn't exist in the provided lists, mark it as "NOT FOUND IN CODE" - never guess
4. Your job is to MATCH existing columns and describe their transformations - NOT to create new ones

**SOURCE ({source_system.upper()}) COLUMNS (extracted from code):**
{source_col_list if source_col_list else "  (No columns extracted)"}

**TARGET (DATABRICKS) COLUMNS (extracted from code):**
{target_col_list if target_col_list else "  (No columns extracted)"}

**TASK:**
For each TARGET column, identify:
1. Which SOURCE column(s) it maps to (by semantic meaning, not just name)
2. Whether it's a direct copy, transformation, or new column
3. A brief transformation note (e.g., "Direct copy", "Split from composite_id", "Trimmed and cast")

Return a JSON array of mappings:
[
  {{
    "target_column": "column_name_from_target_list",
    "source_columns": ["column_name_from_source_list"],
    "mapping_type": "direct|transformation|new|removed",
    "transformation_note": "brief description",
    "confidence": 0.0-1.0
  }}
]

**EXAMPLE (if source had FN, LN and target had FirstName, LastName):**
[
  {{"target_column": "FirstName", "source_columns": ["FN"], "mapping_type": "direct", "transformation_note": "Column renamed from FN", "confidence": 0.95}},
  {{"target_column": "LastName", "source_columns": ["LN"], "mapping_type": "direct", "transformation_note": "Column renamed from LN", "confidence": 0.95}}
]

Generate mappings now (JSON only, no extra text):"""

        try:
            # Call AI analyzer using analyze_code method
            response = self.ai_analyzer.analyze_code(
                code=prompt,
                context="Column mapping generation",
                analysis_type="lineage_extraction"
            )

            # Parse JSON response
            import json
            import re

            # Extract JSON from response (handle markdown code blocks)
            json_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find raw JSON array
                json_match = re.search(r'(\[.*\])', response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    logger.warning("      ⚠ Could not extract JSON from AI response")
                    return self._generate_simple_mappings(source_schema, target_schema)

            mappings = json.loads(json_str)

            logger.info(f"      ✅ AI generated {len(mappings)} column mappings")
            return mappings

        except Exception as e:
            logger.error(f"      ❌ AI mapping failed: {e}")
            # Fallback to simple name-based matching
            return self._generate_simple_mappings(source_schema, target_schema)

    def _generate_simple_mappings(
        self,
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Fallback: Generate simple name-based column mappings (no AI)
        """
        logger.info("      Using simple name-based matching (no AI)")

        source_columns = {col.get('name').lower(): col for col in source_schema.get('columns', [])}
        target_columns = source_schema.get('columns', [])

        mappings = []
        for target_col in target_columns:
            target_name = target_col.get('name', '')
            target_name_lower = target_name.lower()

            # Try exact match
            if target_name_lower in source_columns:
                mappings.append({
                    'target_column': target_name,
                    'source_columns': [source_columns[target_name_lower].get('name')],
                    'mapping_type': 'direct',
                    'transformation_note': 'Direct copy (exact name match)',
                    'confidence': 1.0
                })
            else:
                # Mark as new column
                mappings.append({
                    'target_column': target_name,
                    'source_columns': [],
                    'mapping_type': 'new',
                    'transformation_note': 'New column in target (no source match)',
                    'confidence': 0.5
                })

        return mappings

    def _generate_column_mappings_with_rag(
        self,
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any],
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any],
        source_system: str
    ) -> List[Dict[str, Any]]:
        """
        Generate column mappings using advanced RAG

        Uses:
        - Semantic similarity (not just name matching)
        - Transformation logic inference
        - Context from both workflows
        - Confidence scoring
        """
        logger.info("   🔗 Generating column mappings with RAG")

        # Prepare schemas for prompt
        source_columns_str = json.dumps(source_schema.get('columns', []), indent=2)
        target_columns_str = json.dumps(target_schema.get('columns', []), indent=2)

        # Get table names and escape any curly braces
        source_table = str(source_schema.get('table_name', 'Unknown')).replace('{', '{{').replace('}', '}}')
        target_table = str(target_schema.get('table_name', 'Unknown')).replace('{', '{{').replace('}', '}}')

        # Escape curly braces in JSON strings to avoid f-string interpolation issues
        source_columns_str = source_columns_str.replace('{', '{{').replace('}', '}}')
        target_columns_str = target_columns_str.replace('{', '{{').replace('}', '}}')

        # Get schema names
        source_schema_name = source_schema.get('schema', f"{source_system.upper()}_SCHEMA")
        target_schema_name = target_schema.get('schema', "DATABRICKS_SCHEMA")

        # ENHANCED RAG prompt for 13-column STTM mapping
        prompt = f"""You are a data mapping expert creating COMPLETE Source-to-Target Mappings (STTM) in 13-column Excel format for {source_system} to Databricks migration.

# SOURCE SCHEMA
System: {source_system}
Schema: {source_schema_name}
Table: {source_table}

Columns:
{source_columns_str}

# TARGET SCHEMA
System: Databricks
Schema: {target_schema_name}
Table: {target_table}

Columns:
{target_columns_str}

# TASK: Generate COMPLETE 13-Column STTM for EVERY Target Column

For EACH target column, create a mapping with 13 columns:

1. **processing_order**: Sequential number (1, 2, 3...)
2. **schema**: Schema name (e.g., "DATABRICKS_BDF")
3. **target_table**: Target table name
4. **target_field**: Target column name
5. **data_type**: Data type (STRING, INTEGER, DECIMAL, DATE, etc.)
6. **is_pk**: Is primary key? (true/false)
7. **contains_pii**: Contains PII? (true/false)
8. **field_type**: Classification (Identifier, Demographic, Financial, Calculated, Reference, Status, Metadata)
9. **field_depends_on**: Source columns this depends on (comma-separated string, or empty if direct)
10. **pre_processing_rules**: DETAILED transformation logic with:
    - Activity/Component reference (e.g., "ACTIVITY: Parse & Split.")
    - Exact transformation formula (e.g., "Extracts first part of composite ID by splitting on '_'. Formula: SPLIT(composite_id, '_')[0]")
11. **source_field_names**: Source field expression (e.g., "value.substr(1,64)" or column name)
12. **source_dataset**: Source file/table name
13. **field_definition**: Business definition (what this field represents)

# OUTPUT FORMAT (JSON Array):

[
  {{
    "processing_order": 1,
    "schema": "{target_schema_name}",
    "target_table": "{target_table}",
    "target_field": "hospitalfk",
    "data_type": "SHORT",
    "is_pk": false,
    "contains_pii": false,
    "field_type": "Identifier",
    "field_depends_on": "ID",
    "pre_processing_rules": "ACTIVITY: Parse & Split. Extracts first part of composite ID by splitting on '_'. Formula: SPLIT(composite_id, '_')[0]",
    "source_field_names": "value.substr(1,64)",
    "source_dataset": "*_cbeMatchAppend.dat",
    "field_definition": "A new foreign key for the hospital, derived from the original composite ID."
  }},
  {{
    "processing_order": 2,
    "schema": "{target_schema_name}",
    "target_table": "{target_table}",
    "target_field": "patient_acct_id",
    "data_type": "STRING",
    "is_pk": false,
    "contains_pii": true,
    "field_type": "Identifier",
    "field_depends_on": "ID",
    "pre_processing_rules": "ACTIVITY: Parse & Split. Extracts second part of composite ID. Formula: SPLIT(composite_id, '_')[1]",
    "source_field_names": "value.substr(1,64)",
    "source_dataset": "*_cbeMatchAppend.dat",
    "field_definition": "Patient account identifier from BDF result file."
  }}
]
```

# CRITICAL REQUIREMENTS:

- **Generate mapping for EVERY target column** (aim for 50-100+ rows)
- **pre_processing_rules MUST be DETAILED**:
  - Include activity/component name
  - Include exact transformation formula/expression
  - Example: "ACTIVITY: Reformat_Component. Concatenates first and last name with space. Formula: CONCAT(first_name, ' ', last_name)"
- **field_depends_on**: List all source columns (comma-separated string)
- **source_field_names**: Can be expression or column name
- **source_dataset**: Actual source file/table name
- **field_definition**: Clear business explanation

# TRANSFORMATION FORMULA EXAMPLES:

- CONCAT: `CONCAT(first_name, ' ', last_name)`
- SPLIT: `SPLIT(full_name, '_')[0]`
- CAST: `CAST(amount AS DECIMAL(10,2))`
- SUBSTR: `SUBSTR(value, 1, 64)` or `value.substr(1,64)`
- CASE: `CASE WHEN age >= 18 THEN 'Adult' ELSE 'Minor' END`
- COALESCE: `COALESCE(phone_mobile, phone_home, 'N/A')`
- DIRECT: Column name (no transformation)

Now generate the COMPLETE 13-column STTM for ALL target columns:
"""

        try:
            ai_response = self.ai_analyzer.analyze_code(
                code=prompt,
                context="STTM mapping generation",
                analysis_type="lineage_extraction"
            )
            mappings = self._parse_mappings_response(ai_response)

            if mappings:
                # Validate mappings
                mappings = self._validate_mappings(mappings, target_schema)
                logger.info(f"   ✅ Generated {len(mappings)} validated mappings")
                return mappings
            else:
                logger.warning("   ⚠ No mappings generated by AI")
                return []

        except Exception as e:
            logger.error(f"Mapping generation failed: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    def _parse_mappings_response(self, ai_response: str) -> List[Dict[str, Any]]:
        """Parse AI mapping generation response"""
        try:
            # Extract JSON array
            json_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r'\[.*\]', ai_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    return []

            mappings = json.loads(json_str)

            if isinstance(mappings, list):
                return mappings
            else:
                return []

        except Exception as e:
            logger.warning(f"Failed to parse mappings response: {e}")
            return []

    def _calculate_processing_order(self, mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate dependency-based processing order

        Rules:
        1. Columns with no dependencies get lowest order
        2. Columns processed together (same transformation) get same order
        3. Columns that depend on others get higher order

        Returns updated mappings with processing_order set
        """
        # Build dependency graph
        field_to_mapping = {m.get('target_field', ''): m for m in mappings}

        # Calculate dependency depth for each field
        def get_dependency_depth(field_name, visited=None):
            """Recursively calculate max dependency depth"""
            if visited is None:
                visited = set()

            if field_name in visited:
                return 0  # Circular dependency

            mapping = field_to_mapping.get(field_name)
            if not mapping:
                return 0

            depends_on = mapping.get('field_depends_on', '')
            if not depends_on or depends_on == 'None' or depends_on == '':
                return 1  # Base level - no dependencies

            visited.add(field_name)

            # Parse dependencies (can be comma-separated)
            deps = [d.strip() for d in str(depends_on).split(',') if d.strip()]
            max_depth = 0

            for dep in deps:
                if dep in field_to_mapping:
                    dep_depth = get_dependency_depth(dep, visited.copy())
                    max_depth = max(max_depth, dep_depth)

            return max_depth + 1

        # Assign processing order based on dependency depth
        for mapping in mappings:
            field_name = mapping.get('target_field', '')
            depth = get_dependency_depth(field_name)

            # Multiply by 10 to leave room for same-transformation grouping
            mapping['processing_order'] = depth * 10

        # Group fields with same transformation (same order)
        # Fields from same source line/transformation should have same order
        transformation_groups = {}
        for mapping in mappings:
            transformation = mapping.get('pre_processing_rules', '')
            source_line = mapping.get('source_line', 0)

            # Create key from transformation + source location
            group_key = (transformation[:100] if transformation else '', source_line)

            if group_key not in transformation_groups:
                transformation_groups[group_key] = []
            transformation_groups[group_key].append(mapping)

        # Assign same order to fields in same transformation group
        for group_mappings in transformation_groups.values():
            if len(group_mappings) > 1:
                # All fields in this group get the minimum order from the group
                min_order = min(m['processing_order'] for m in group_mappings)
                for m in group_mappings:
                    m['processing_order'] = min_order

        return mappings

    def _validate_mappings(
        self,
        mappings: List[Dict[str, Any]],
        target_schema: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Validate and enrich 13-column STTM mappings"""
        validated = []

        target_columns = {col['name'] for col in target_schema.get('columns', [])}

        # Calculate dependency-based processing order
        mappings = self._calculate_processing_order(mappings)

        for i, mapping in enumerate(mappings, 1):
            # Ensure processing_order exists (should be set by _calculate_processing_order)
            if 'processing_order' not in mapping or not mapping['processing_order']:
                mapping['processing_order'] = i

            # Validate required 13-column fields
            required_fields = ['target_field', 'data_type', 'pre_processing_rules']
            if not all(key in mapping for key in required_fields):
                logger.warning(f"   ⚠ Mapping missing required fields: {mapping.get('target_field', 'Unknown')}")
                continue

            # Ensure all 13 columns exist with defaults
            mapping.setdefault('schema', target_schema.get('schema', 'DATABRICKS_SCHEMA'))
            mapping.setdefault('target_table', target_schema.get('table_name', 'Unknown'))
            mapping.setdefault('is_pk', False)
            mapping.setdefault('contains_pii', False)
            mapping.setdefault('field_type', 'Unknown')
            mapping.setdefault('field_depends_on', '')
            mapping.setdefault('source_field_names', '')
            mapping.setdefault('source_dataset', 'Unknown')
            mapping.setdefault('field_definition', '')

            # Ensure boolean fields are actually boolean
            mapping['is_pk'] = bool(mapping.get('is_pk', False))
            mapping['contains_pii'] = bool(mapping.get('contains_pii', False))

            validated.append(mapping)

        return validated

    def _fallback_sttm_generation(
        self,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Fallback STTM generation without AI"""
        logger.info("   Using fallback STTM generation")

        # Extract output names from both systems
        source_outputs = self._extract_output_names(source_logic)
        target_outputs = self._extract_output_names(databricks_logic)

        mappings = []

        # Create simple mappings for common outputs
        for target_output in target_outputs:
            # Try to find matching source output
            matched_source = None
            for source_output in source_outputs:
                if source_output.lower() in target_output.lower() or target_output.lower() in source_output.lower():
                    matched_source = source_output
                    break

            if matched_source:
                mappings.append({
                    'target_column': target_output,
                    'data_type': 'STRING',
                    'source_columns': [matched_source],
                    'transformation_logic': 'DIRECT',
                    'dependencies': [],
                    'confidence': 0.5
                })
            else:
                mappings.append({
                    'target_column': target_output,
                    'data_type': 'STRING',
                    'source_columns': ['Unknown'],
                    'transformation_logic': 'NEW or RENAMED (requires investigation)',
                    'dependencies': [],
                    'confidence': 0.3
                })

        logger.info(f"   ✅ Created {len(mappings)} fallback mappings")
        return mappings

    def _extract_output_names(self, logic: Dict[str, Any]) -> List[str]:
        """Extract output column/field names from logic"""
        outputs = []

        system = logic.get('system', '')

        if system == 'hadoop':
            for job in logic.get('jobs', []):
                outputs.extend(job.get('outputs', []))
        elif system == 'abinitio':
            for step in logic.get('steps', []):
                # Handle both dict and string formats
                if isinstance(step, dict):
                    outputs.extend(step.get('outputs', []))
        elif system == 'databricks':
            for activity in logic.get('activities', []):
                outputs.extend(activity.get('outputs', []))

        return list(set(outputs))[:20]  # Limit to 20 unique outputs


# Example usage
if __name__ == "__main__":
    # Test without AI
    generator = STAGSTTMGenerator()

    print("\n" + "=" * 80)
    print("STAG STTM GENERATOR TEST")
    print("=" * 80)

    # Sample logic
    source_logic = {
        'system': 'abinitio',
        'graph_name': '1300_CDD_PatientAcctsXRefPermID.pset',
        'steps': [
            {'dataset': 'patient_data', 'outputs': ['patient_id', 'name', 'dob']}
        ],
        'dml_files': ['patient.dml']
    }

    databricks_logic = {
        'system': 'databricks',
        'pipeline_name': 'pl_cdd_bdf_download',
        'activities': [
            {'name': 'Load_Patient', 'outputs': ['patient_id', 'full_name', 'birth_date']}
        ]
    }

    print("\nNote: Without AI analyzer, using fallback mapping")

    mappings = generator.generate_sttm(source_logic, databricks_logic, 'abinitio')

    print(f"\n✅ Generated {len(mappings)} STTM mappings:")
    for i, mapping in enumerate(mappings, 1):
        print(f"\n{i}. {mapping['target_column']} ({mapping['data_type']})")
        print(f"   Source: {', '.join(mapping['source_columns'])}")
        print(f"   Transform: {mapping['transformation_logic']}")
        print(f"   Confidence: {mapping['confidence']}")

    print("\n" + "=" * 80)
    print("Note: Full functionality requires AI analyzer for best RAG results")
    print("=" * 80)
