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

    def generate_sttm(
        self,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any],
        source_system: str
    ) -> List[Dict[str, Any]]:
        """
        Generate Source-to-Target Mappings

        Args:
            source_logic: Logic from Hadoop or Ab Initio
            databricks_logic: Logic from Databricks
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
        logger.info(f"🔗 Generating STTM from {source_system} to Databricks")

        if not self.ai_analyzer:
            logger.warning("No AI analyzer provided - using fallback STTM")
            return self._fallback_sttm_generation(source_logic, databricks_logic)

        try:
            # Step 1: Extract schemas using advanced RAG
            source_schema = self._extract_schema_with_rag(source_logic, source_system)
            target_schema = self._extract_schema_with_rag(databricks_logic, 'databricks')

            if not source_schema or not target_schema:
                logger.warning("Failed to extract schemas - using fallback")
                return self._fallback_sttm_generation(source_logic, databricks_logic)

            # Step 2: Generate column mappings using AI
            mappings = self._generate_column_mappings_with_rag(
                source_schema,
                target_schema,
                source_logic,
                databricks_logic,
                source_system
            )

            logger.info(f"✅ Generated {len(mappings)} column mappings")
            return mappings

        except Exception as e:
            logger.error(f"STTM generation failed: {e}")
            return self._fallback_sttm_generation(source_logic, databricks_logic)

    def _extract_schema_with_rag(
        self,
        logic: Dict[str, Any],
        system: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract schema using RAG with schema-specific prompting

        Returns:
            {
                'table_name': str,
                'columns': [
                    {'name': str, 'type': str, 'description': str}
                ],
                'source_files': List[str]
            }
        """
        logger.info(f"   📋 Extracting schema for {system} using RAG")

        # Create context for schema extraction
        context = self._create_schema_context(logic, system)

        # Get workflow name and escape any curly braces
        workflow_name = str(logic.get('workflow_name') or logic.get('graph_name') or logic.get('pipeline_name', 'Unknown'))
        workflow_name = workflow_name.replace('{', '{{').replace('}', '}}')

        # Escape any curly braces in context to avoid f-string interpolation issues
        context = context.replace('{', '{{').replace('}', '}}')

        # Advanced RAG prompt for schema extraction
        prompt = f"""You are a data schema expert analyzing {system} workflows to extract table schemas.

# WORKFLOW INFORMATION
System: {system}
Workflow: {workflow_name}

{context}

# TASK: Extract Output Schema

Analyze this workflow and extract the OUTPUT schema (the final data structure produced).

Look for:
1. **Column definitions** in DML files, CREATE TABLE statements, DataFrame schemas
2. **Data types** (STRING, INTEGER, DECIMAL, DATE, etc.)
3. **Transformation logic** that creates new columns
4. **Source file references** (DML files, schema definitions)

# OUTPUT FORMAT

Return a JSON object with this structure:

```json
{{
  "table_name": "output_table_name",
  "columns": [
    {{
      "name": "column_name",
      "type": "data_type",
      "description": "Brief description of what this column contains"
    }}
  ],
  "source_files": ["file1.dml", "schema.py"]
}}
```

# GUIDELINES

- **Infer from context**: If explicit schema not found, infer from transformation logic
- **Data types**: Use standard types (STRING, INTEGER, DECIMAL, DATE, TIMESTAMP, BOOLEAN)
- **Descriptions**: Explain business meaning, not just technical details
- **Source files**: List DML, XFR, Python, or SQL files that define schema

# EXAMPLE (for reference only):

```json
{{
  "table_name": "customer_output",
  "columns": [
    {{"name": "customer_id", "type": "STRING", "description": "Unique customer identifier"}},
    {{"name": "full_name", "type": "STRING", "description": "Customer full name (first + last)"}},
    {{"name": "account_balance", "type": "DECIMAL", "description": "Current account balance"}},
    {{"name": "last_updated", "type": "TIMESTAMP", "description": "Last record update timestamp"}}
  ],
  "source_files": ["customer.dml", "transform_customer.xfr"]
}}
```

Now extract the schema from the workflow above:
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
                output_steps = [s for s in steps if 'output' in s.get('component_type', '').lower()]
                for step in output_steps[:3]:
                    context_parts.append(f"  - {step['dataset']}: {step.get('transformation_rules', '')}")

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

        # Advanced RAG prompt for column mapping
        prompt = f"""You are a data mapping expert creating Source-to-Target Mappings (STTM) for a {source_system} to Databricks migration.

# SOURCE SCHEMA
System: {source_system}
Table: {source_table}

Columns:
{source_columns_str}

# TARGET SCHEMA
System: Databricks
Table: {target_table}

Columns:
{target_columns_str}

# TASK: Generate Column Mappings

For EACH target column, identify:
1. **Source columns** that map to it (can be multiple for derived columns)
2. **Transformation logic** (if any)
3. **Dependencies** (lookup tables, reference files)
4. **Confidence** (how certain you are about the mapping)

# MAPPING RULES

- **Direct Mapping**: Same column exists in source → DIRECT COPY
- **Derived Mapping**: Target column computed from multiple source columns → Show formula
- **Renamed Mapping**: Same data, different name → RENAME
- **New Column**: Target column doesn't exist in source → NEW (explain default value)
- **Type Conversion**: Source and target have different types → CAST

# OUTPUT FORMAT

Return a JSON array with this structure:

```json
[
  {{
    "target_column": "column_name",
    "data_type": "STRING",
    "source_columns": ["source_col1", "source_col2"],
    "transformation_logic": "CONCAT(source_col1, ' ', source_col2)",
    "dependencies": ["reference_table.dml"],
    "confidence": 0.95
  }}
]
```

# TRANSFORMATION LOGIC EXAMPLES

- **CONCAT**: `CONCAT(first_name, ' ', last_name)` → full_name
- **SPLIT**: `SPLIT(full_name, ' ')[0]` → first_name
- **CAST**: `CAST(amount AS DECIMAL)` → Converts type
- **LOOKUP**: `LOOKUP(customer_id, customer_ref_table)` → Gets value from reference
- **COALESCE**: `COALESCE(phone_mobile, phone_home, 'N/A')` → First non-null value
- **CASE**: `CASE WHEN age >= 18 THEN 'Adult' ELSE 'Minor' END` → Conditional logic
- **DATE_FORMAT**: `DATE_FORMAT(created_date, 'yyyy-MM-dd')` → Format conversion
- **DIRECT**: `customer_id` → No transformation (direct copy)

# CONFIDENCE SCORING

- **0.95-1.0**: Exact match (same name and type)
- **0.80-0.94**: High confidence (same data, minor differences)
- **0.60-0.79**: Medium confidence (inferred from context)
- **0.40-0.59**: Low confidence (educated guess)
- **0.20-0.39**: Very uncertain (needs validation)

# IMPORTANT

- **Be specific**: Use actual column names from schemas above
- **Explain transformations**: Don't just say "transformation" - show the formula
- **List all sources**: If multiple columns contribute, list them all
- **Include dependencies**: Reference lookup files, DML files, etc.

Now generate the STTM mappings:
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

    def _validate_mappings(
        self,
        mappings: List[Dict[str, Any]],
        target_schema: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Validate and enrich mappings"""
        validated = []

        target_columns = {col['name'] for col in target_schema.get('columns', [])}

        for mapping in mappings:
            # Validate required fields
            if not all(key in mapping for key in ['target_column', 'source_columns', 'transformation_logic']):
                logger.warning(f"   ⚠ Mapping missing required fields: {mapping.get('target_column', 'Unknown')}")
                continue

            # Ensure source_columns is a list
            if not isinstance(mapping.get('source_columns'), list):
                mapping['source_columns'] = [mapping['source_columns']]

            # Ensure dependencies is a list
            if 'dependencies' not in mapping:
                mapping['dependencies'] = []
            elif not isinstance(mapping['dependencies'], list):
                mapping['dependencies'] = [mapping['dependencies']]

            # Validate confidence
            confidence = mapping.get('confidence', 0.7)
            if not isinstance(confidence, (int, float)) or not (0 <= confidence <= 1):
                mapping['confidence'] = 0.7

            # Ensure data_type exists
            if 'data_type' not in mapping:
                mapping['data_type'] = 'STRING'

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
