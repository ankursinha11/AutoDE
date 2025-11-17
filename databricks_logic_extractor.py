"""
Databricks Logic Extractor

Extracts logic and transformations from Databricks pipelines using:
1. ADF pipeline JSON analysis (orchestration flow)
2. Databricks notebook search (transformation logic)
3. AI analysis for code snippet extraction
4. Activity dependency mapping

Handles:
- ADF pipeline orchestration
- Databricks notebook activities
- Data source and sink identification
- Transformation logic extraction (Python/SQL)
- Conditional branches and loops
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
from loguru import logger
import json
import re


class DatabricksLogicExtractor:
    """Extract logic from Databricks pipelines using ADF JSON and AI analysis"""

    def __init__(self, indexer=None, ai_analyzer=None):
        """
        Initialize Databricks Logic Extractor

        Args:
            indexer: MultiCollectionIndexer for vector search
            ai_analyzer: AIAnalyzer for natural language extraction
        """
        self.indexer = indexer
        self.ai_analyzer = ai_analyzer

    def extract_logic(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Extract comprehensive logic from Databricks pipeline

        Args:
            pipeline_name: Name of the Databricks/ADF pipeline

        Returns:
            {
                'pipeline_name': str,
                'system': 'databricks',
                'activities': [
                    {
                        'name': str,
                        'notebook': str,
                        'purpose': str,
                        'code_snippets': List[str],
                        'inputs': List[str],
                        'outputs': List[str],
                        'transformations': List[str]
                    }
                ],
                'orchestration_flow': str,
                'conditional_branches': Dict[str, List[str]],
                'total_activities': int
            }
        """
        logger.info(f"📊 Extracting logic for Databricks pipeline: {pipeline_name}")

        # Step 1: Search for pipeline documents in vector DB
        pipeline_docs = self._search_pipeline_documents(pipeline_name)

        if not pipeline_docs:
            logger.warning(f"No documents found for pipeline: {pipeline_name}")
            return self._create_empty_result(pipeline_name)

        # Step 2: Find ADF JSON file
        adf_json_path = self._find_adf_json(pipeline_docs, pipeline_name)

        # Step 3: Parse ADF pipeline if available
        if adf_json_path:
            activities = self._extract_activities_from_adf(adf_json_path)
            orchestration_flow = self._extract_orchestration_flow(adf_json_path)
            conditional_branches = self._extract_conditional_branches(adf_json_path)
        else:
            # Fallback: Extract from documents using AI
            activities = self._extract_activities_with_ai(pipeline_docs, pipeline_name)
            orchestration_flow = self._build_orchestration_flow_from_activities(activities)
            conditional_branches = {}

        # Step 4: Enrich activities with notebook code (if available)
        activities = self._enrich_activities_with_notebooks(activities)

        result = {
            'pipeline_name': pipeline_name,
            'system': 'databricks',
            'activities': activities,
            'orchestration_flow': orchestration_flow,
            'conditional_branches': conditional_branches,
            'total_activities': len(activities)
        }

        logger.info(f"✅ Extracted {len(activities)} activities from Databricks pipeline: {pipeline_name}")
        return result

    def _search_pipeline_documents(self, pipeline_name: str) -> List[Dict[str, Any]]:
        """Search for pipeline documents in vector DB"""
        if not self.indexer:
            logger.warning("No indexer provided - cannot search for pipeline documents")
            return []

        try:
            # Search in databricks_collection and adf_collection
            search_results = self.indexer.search_multi_collection(
                query=f"Databricks pipeline {pipeline_name} ADF activities notebooks",
                collections=["databricks_collection", "adf_collection"],
                top_k=20
            )

            databricks_docs = search_results.get('databricks_collection', [])
            adf_docs = search_results.get('adf_collection', [])

            all_docs = databricks_docs + adf_docs
            logger.info(f"   Found {len(all_docs)} Databricks/ADF documents for {pipeline_name}")

            return all_docs

        except Exception as e:
            logger.error(f"Failed to search pipeline documents: {e}")
            return []

    def _find_adf_json(self, pipeline_docs: List[Dict[str, Any]], pipeline_name: str) -> Optional[str]:
        """
        Find ADF JSON file path - FORCE direct file system search as primary method

        This bypasses unreliable vector DB searches and directly locates ADF pipeline JSONs.
        """
        import glob

        # Strategy 1: Direct file system search (PRIMARY METHOD - most reliable)
        logger.info(f"   🔍 Searching for ADF JSON: {pipeline_name}")

        # Known ADF pipeline base locations
        base_paths = [
            "/Users/ankurshome/Desktop/Hadoop_Parser/CodebaseIntelligence/Databricks_repo/app-insleads-adf/adf/pipeline",
            "/Users/ankurshome/Desktop/Hadoop_Parser/CodebaseIntelligence/Databricks_repo/*/adf/pipeline",
        ]

        for base_path in base_paths:
            # Try exact name match first
            exact_path = f"{base_path}/{pipeline_name}.json"
            if '*' not in exact_path and Path(exact_path).exists():
                logger.info(f"   ✅ FOUND (exact match): {exact_path}")
                return exact_path

            # Try glob pattern for wildcard paths
            if '*' in base_path:
                pattern = f"{base_path}/{pipeline_name}.json"
                matches = glob.glob(pattern)
                if matches:
                    logger.info(f"   ✅ FOUND (glob match): {matches[0]}")
                    return matches[0]

        # Strategy 2: Fuzzy search - check all JSON files in directory
        logger.info(f"   Trying fuzzy search for {pipeline_name}...")

        for base_path in base_paths:
            if '*' in base_path:
                # Expand wildcard first
                expanded_dirs = glob.glob(base_path)
                search_dirs = expanded_dirs
            else:
                search_dirs = [base_path]

            for search_dir in search_dirs:
                if not Path(search_dir).exists():
                    continue

                all_jsons = glob.glob(f"{search_dir}/*.json")

                for json_path in all_jsons:
                    file_stem = Path(json_path).stem

                    # Check if pipeline name (without underscores) matches file name (without underscores)
                    normalized_pipeline = pipeline_name.replace("_", "").replace("-", "").lower()
                    normalized_file = file_stem.replace("_", "").replace("-", "").lower()

                    if normalized_pipeline == normalized_file:
                        logger.info(f"   ✅ FOUND (fuzzy match): {json_path}")
                        return json_path

                    # Partial match
                    if normalized_pipeline in normalized_file or normalized_file in normalized_pipeline:
                        logger.info(f"   ✅ FOUND (partial match): {json_path}")
                        return json_path

        # Strategy 3: Fallback to vector DB (least reliable)
        logger.info(f"   Trying vector DB fallback...")
        for doc in pipeline_docs:
            metadata = doc.get('metadata', {})
            file_path = metadata.get('file_path', '') or metadata.get('absolute_file_path', '')
            file_name = metadata.get('file_name', '')

            if file_name.lower() == f"{pipeline_name.lower()}.json":
                if Path(file_path).exists():
                    logger.info(f"   ✅ Found ADF JSON (vector DB): {file_path}")
                    return file_path

        # CRITICAL ERROR - file must exist for production
        logger.error(f"   ❌ CRITICAL: ADF JSON NOT FOUND for '{pipeline_name}'")
        logger.error(f"   Searched directories:")
        for path in base_paths:
            logger.error(f"     - {path}")

        # List available files for debugging
        for base_path in base_paths:
            if '*' not in base_path and Path(base_path).exists():
                available = [f.name for f in Path(base_path).glob("*.json")]
                logger.error(f"   Available JSONs in {base_path}:")
                for fname in available[:10]:  # Show first 10
                    logger.error(f"     - {fname}")

        return None

    def _extract_activities_from_adf(self, adf_json_path: str) -> List[Dict[str, Any]]:
        """Extract activities from ADF pipeline JSON"""
        try:
            with open(adf_json_path, 'r') as f:
                adf_data = json.load(f)

            activities = []

            # Navigate to activities array
            # Typical ADF structure: { "properties": { "activities": [...] } }
            adf_activities = adf_data.get('properties', {}).get('activities', [])

            for activity in adf_activities:
                activity_name = activity.get('name', 'Unknown')
                activity_type = activity.get('type', 'Unknown')

                # Extract Databricks notebook info
                notebook_path = self._extract_notebook_path(activity)

                # Extract inputs/outputs
                inputs = self._extract_activity_inputs(activity)
                outputs = self._extract_activity_outputs(activity)

                # Extract transformation description
                transformations = self._extract_transformations_from_activity(activity)

                activities.append({
                    'name': activity_name,
                    'type': activity_type,
                    'notebook': notebook_path,
                    'purpose': self._get_activity_purpose(activity_type, activity_name),
                    'code_snippets': [],  # Will be enriched later
                    'inputs': inputs,
                    'outputs': outputs,
                    'transformations': transformations
                })

            logger.info(f"   Extracted {len(activities)} activities from ADF JSON")
            return activities

        except Exception as e:
            logger.error(f"Failed to extract activities from ADF JSON: {e}")
            return []

    def _extract_notebook_path(self, activity: Dict[str, Any]) -> str:
        """Extract Databricks notebook path from activity"""
        # Look in typeProperties.notebookPath
        notebook_path = activity.get('typeProperties', {}).get('notebookPath', {})

        if isinstance(notebook_path, dict):
            # It's a dynamic expression
            return notebook_path.get('value', 'Unknown')
        elif isinstance(notebook_path, str):
            return notebook_path
        else:
            return 'No notebook'

    def _extract_activity_inputs(self, activity: Dict[str, Any]) -> List[str]:
        """
        Extract input datasets from activity with enhanced code-based extraction

        Handles patterns like:
        - df = spark.read.format("delta").load("/mnt/adls/bronze/table_name")
        - df = spark.table("database.table_name")
        - df = spark.read.parquet("/path/to/input")
        """
        inputs = []

        # Check dependsOn for input activities
        depends_on = activity.get('dependsOn', [])
        for dep in depends_on:
            activity_name = dep.get('activity', 'Unknown')
            inputs.append(f"Output of {activity_name}")

        # Check inputs array
        activity_inputs = activity.get('inputs', [])
        for input_ref in activity_inputs:
            ref_name = input_ref.get('referenceName', 'Unknown')
            inputs.append(ref_name)

        # For DatabricksNotebook activities, try to extract inputs from notebook code
        if activity.get('type') == 'DatabricksNotebook':
            notebook_path = self._extract_notebook_path(activity)
            if notebook_path:
                notebook_inputs = self._extract_inputs_from_notebook_code(notebook_path)
                inputs.extend(notebook_inputs)

        # Remove duplicates
        return list(set([i for i in inputs if i]))

    def _extract_inputs_from_notebook_code(self, notebook_path: str) -> List[str]:
        """
        Extract input table names from Databricks notebook code

        Returns:
            List of table names (e.g., ["nopermid", "patient_accounts"])
        """
        if not self.indexer:
            return []

        try:
            # Search for notebook code
            search_results = self.indexer.search_multi_collection(
                query=f"Databricks notebook {notebook_path} read load input table",
                collections=["databricks_collection"],
                top_k=20  # Increased from 10 to 20 to ensure we get all notebook chunks
            )

            notebook_docs = search_results.get('databricks_collection', [])
            inputs = []

            for doc in notebook_docs:
                content = doc.get('content', '')

                # Pattern 1: .load("/path/to/table") or .table("table_name")
                read_patterns = [
                    r'\.load\([\'"]([^\'"]+)[\'"]\)',
                    r'\.table\([\'"]([^\'"]+)[\'"]\)',
                    r'spark\.read\.[^(]+\([\'"]([^\'"]+)[\'"]\)',
                ]

                for pattern in read_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        # Extract table name from path
                        cleaned = match.strip()

                        # If it's a database.table format, take the table part
                        if '.' in cleaned and '/' not in cleaned:
                            parts = cleaned.split('.')
                            table_name = parts[-1]
                            inputs.append(table_name)
                        else:
                            # It's a path - extract meaningful table name
                            parts = [p.strip() for p in cleaned.split('/') if p.strip()]

                            # Look for table name (last meaningful part)
                            for part in reversed(parts):
                                # Skip date patterns, bc variables, file extensions
                                part_cleaned = re.sub(r'\.(parquet|csv|json|delta|dat|txt)$', '', part, flags=re.IGNORECASE)
                                if part_cleaned and len(part_cleaned) > 2 and not re.match(r'^(bc|date|\d{6,8})$', part_cleaned, re.IGNORECASE):
                                    inputs.append(part_cleaned)
                                    break

                # Pattern 2: SQL FROM/JOIN clauses
                sql_from_patterns = [
                    r'FROM\s+([a-zA-Z_][\w.]*)',
                    r'JOIN\s+([a-zA-Z_][\w.]*)',
                ]

                for pattern in sql_from_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        # Skip SQL keywords
                        if match.upper() in ['SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LATERAL', 'USING', 'ON']:
                            continue

                        # Extract table name (might be database.table)
                        if '.' in match:
                            parts = match.split('.')
                            table_name = parts[-1]
                            inputs.append(table_name)
                        else:
                            inputs.append(match)

            # Remove duplicates and return
            unique_inputs = list(set([i for i in inputs if i]))
            if unique_inputs:
                logger.info(f"      Extracted {len(unique_inputs)} input tables from {notebook_path}: {unique_inputs}")

            return unique_inputs[:10]  # Limit to 10 inputs

        except Exception as e:
            logger.error(f"      Failed to extract inputs from notebook code: {e}")
            return []

    def _extract_activity_outputs(self, activity: Dict[str, Any]) -> List[str]:
        """
        Extract output datasets from activity with enhanced code-based extraction

        Handles patterns like:
        - df.write.format("delta").save("/mnt/adls/silver/table_name")
        - df.write.saveAsTable("database.table_name")
        - df.write.format("parquet").mode("overwrite").save("/path/to/output")
        """
        outputs = []

        # Check outputs array from ADF JSON
        activity_outputs = activity.get('outputs', [])
        for output_ref in activity_outputs:
            ref_name = output_ref.get('referenceName', 'Unknown')
            outputs.append(ref_name)

        # Check typeProperties for sink/output references
        sink = activity.get('typeProperties', {}).get('sink', {})
        if sink:
            sink_type = sink.get('type', '')
            if sink_type:
                outputs.append(f"Sink: {sink_type}")

        # For DatabricksNotebook activities, try to extract outputs from notebook code
        if activity.get('type') == 'DatabricksNotebook':
            notebook_path = self._extract_notebook_path(activity)
            if notebook_path:
                notebook_outputs = self._extract_outputs_from_notebook_code(notebook_path)
                outputs.extend(notebook_outputs)

        # Remove duplicates
        return list(set([o for o in outputs if o]))

    def _extract_outputs_from_notebook_code(self, notebook_path: str) -> List[str]:
        """
        Extract output table names from Databricks notebook code

        Returns:
            List of table names (e.g., ["permIdPatientAcctId", "allDistinctRecs"])
        """
        if not self.indexer:
            return []

        try:
            # Search for notebook code
            search_results = self.indexer.search_multi_collection(
                query=f"Databricks notebook {notebook_path} write save output",
                collections=["databricks_collection"],
                top_k=20  # Increased from 10 to 20 to ensure we get all notebook chunks
            )

            notebook_docs = search_results.get('databricks_collection', [])
            outputs = []

            for doc in notebook_docs:
                content = doc.get('content', '')

                # Pattern 1: .save("/path/to/table") or .save('/path/to/table')
                save_patterns = [
                    r'\.save\([\'"]([^\'"]+)[\'"]\)',
                    r'\.saveAsTable\([\'"]([^\'"]+)[\'"]\)',
                ]

                for pattern in save_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        # Extract table name from path
                        # Example: "/mnt/adls/silver/permIdPatientAcctId" -> "permIdPatientAcctId"
                        # Example: "database.table_name" -> "table_name"

                        # Remove common prefixes
                        cleaned = match.strip()

                        # If it's a database.table format, take the table part
                        if '.' in cleaned and '/' not in cleaned:
                            parts = cleaned.split('.')
                            table_name = parts[-1]  # Last part is table name
                            outputs.append(table_name)
                        else:
                            # It's a path - extract meaningful table name
                            parts = [p.strip() for p in cleaned.split('/') if p.strip()]

                            # Look for table name (last meaningful part)
                            for part in reversed(parts):
                                # Skip date patterns, bc variables, etc.
                                if part and len(part) > 2 and not re.match(r'^(bc|date|\d{6,8})$', part, re.IGNORECASE):
                                    outputs.append(part)
                                    break

                # Pattern 2: SQL INSERT statements
                # INSERT INTO table_name or INSERT OVERWRITE TABLE table_name
                sql_insert_patterns = [
                    r'INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?TABLE\s+([a-zA-Z_][\w.]*)',
                    r'INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?([a-zA-Z_][\w.]*)\s+SELECT',
                ]

                for pattern in sql_insert_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        # Extract table name (might be database.table)
                        if '.' in match:
                            parts = match.split('.')
                            table_name = parts[-1]
                            outputs.append(table_name)
                        else:
                            outputs.append(match)

                # Pattern 3: CREATE TABLE statements
                create_table_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][\w.]*)'
                matches = re.findall(create_table_pattern, content, re.IGNORECASE)
                for match in matches:
                    if '.' in match:
                        parts = match.split('.')
                        table_name = parts[-1]
                        outputs.append(table_name)
                    else:
                        outputs.append(match)

            # Remove duplicates and return
            unique_outputs = list(set([o for o in outputs if o]))
            if unique_outputs:
                logger.info(f"      Extracted {len(unique_outputs)} output tables from {notebook_path}: {unique_outputs}")

            return unique_outputs[:10]  # Limit to 10 outputs

        except Exception as e:
            logger.error(f"      Failed to extract outputs from notebook code: {e}")
            return []

    def _extract_transformations_from_activity(self, activity: Dict[str, Any]) -> List[str]:
        """Extract transformation descriptions from activity"""
        transformations = []

        activity_type = activity.get('type', '')

        # Map activity types to transformation descriptions
        type_transformations = {
            'DatabricksNotebook': 'Execute Databricks notebook transformation',
            'Copy': 'Copy data from source to sink',
            'DataFlow': 'Execute data flow transformation',
            'ExecutePipeline': 'Execute child pipeline',
            'Lookup': 'Lookup reference data',
            'GetMetadata': 'Get dataset metadata',
            'IfCondition': 'Conditional branching',
            'ForEach': 'Iterate over items',
            'Wait': 'Wait for duration',
        }

        if activity_type in type_transformations:
            transformations.append(type_transformations[activity_type])

        return transformations

    def _get_activity_purpose(self, activity_type: str, activity_name: str) -> str:
        """Get activity purpose description"""
        # Try to infer purpose from name
        name_lower = activity_name.lower()

        if 'load' in name_lower or 'import' in name_lower:
            return f"Load data ({activity_type})"
        elif 'transform' in name_lower or 'process' in name_lower:
            return f"Transform data ({activity_type})"
        elif 'validate' in name_lower or 'check' in name_lower:
            return f"Validate data ({activity_type})"
        elif 'export' in name_lower or 'write' in name_lower:
            return f"Export data ({activity_type})"
        else:
            return f"Execute {activity_type} activity"

    def _extract_orchestration_flow(self, adf_json_path: str) -> str:
        """Extract orchestration flow from ADF JSON"""
        try:
            with open(adf_json_path, 'r') as f:
                adf_data = json.load(f)

            activities = adf_data.get('properties', {}).get('activities', [])

            # Build dependency graph
            activity_names = [a.get('name', 'Unknown') for a in activities]

            # Simple flow: list activities in order
            if len(activity_names) <= 5:
                return " → ".join(activity_names)
            else:
                return f"{' → '.join(activity_names[:3])} → ... → {activity_names[-1]} ({len(activity_names)} activities)"

        except Exception as e:
            logger.error(f"Failed to extract orchestration flow: {e}")
            return "Orchestration flow not available"

    def _extract_conditional_branches(self, adf_json_path: str) -> Dict[str, List[str]]:
        """Extract conditional branches from ADF JSON"""
        branches = {}

        try:
            with open(adf_json_path, 'r') as f:
                adf_data = json.load(f)

            activities = adf_data.get('properties', {}).get('activities', [])

            for activity in activities:
                if activity.get('type') == 'IfCondition':
                    condition_name = activity.get('name', 'Unknown')

                    # Extract true/false activities
                    if_true = activity.get('typeProperties', {}).get('ifTrueActivities', [])
                    if_false = activity.get('typeProperties', {}).get('ifFalseActivities', [])

                    true_names = [a.get('name', 'Unknown') for a in if_true]
                    false_names = [a.get('name', 'Unknown') for a in if_false]

                    branches[condition_name] = {
                        'true': true_names,
                        'false': false_names
                    }

        except Exception as e:
            logger.error(f"Failed to extract conditional branches: {e}")

        return branches

    def _enrich_activities_with_notebooks(self, activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich activities with notebook code snippets using AI analysis (AI PRIMARY)"""
        if not self.indexer:
            logger.warning("   No indexer available for notebook enrichment")
            return activities

        for activity in activities:
            notebook_path = activity.get('notebook', '')

            if notebook_path and notebook_path != 'No notebook':
                # Search for notebook content
                notebook_docs = self._search_notebook_documents(notebook_path)

                if notebook_docs and self.ai_analyzer and self.ai_analyzer.enabled:
                    # Use AI to extract meaningful code and logic
                    enriched_info = self._extract_notebook_logic_with_ai(notebook_docs, notebook_path)

                    # Update activity with AI-extracted information
                    if enriched_info:
                        activity['code_snippets'] = enriched_info.get('code_snippets', [])
                        activity['step_by_step_logic'] = enriched_info.get('step_by_step_logic', [])  # CRITICAL FIX
                        activity['purpose'] = enriched_info.get('purpose', activity.get('purpose', ''))
                        activity['transformations'] = enriched_info.get('transformations', activity.get('transformations', []))
                        activity['inputs'] = enriched_info.get('inputs', activity.get('inputs', []))
                        activity['outputs'] = enriched_info.get('outputs', activity.get('outputs', []))
                else:
                    # Fallback to basic code snippet extraction
                    code_snippets = self._search_notebook_code(notebook_path)
                    activity['code_snippets'] = code_snippets

        return activities

    def _search_notebook_documents(self, notebook_path: str) -> List[Dict[str, Any]]:
        """Search for notebook documents using vector search"""
        if not self.indexer:
            return []

        try:
            # Search for notebook
            search_results = self.indexer.search_multi_collection(
                query=f"Databricks notebook {notebook_path} code transformation",
                collections=["databricks_collection"],
                top_k=20  # Increased from 5 to 20 to ensure we get all notebook chunks
            )

            notebook_docs = search_results.get('databricks_collection', [])
            logger.info(f"   Found {len(notebook_docs)} documents for notebook: {notebook_path}")
            return notebook_docs

        except Exception as e:
            logger.error(f"Failed to search notebook documents: {e}")
            return []

    def _extract_notebook_logic_with_ai(self, notebook_docs: List[Dict[str, Any]], notebook_path: str) -> Optional[Dict[str, Any]]:
        """Extract notebook logic using DEEP AI analysis with step-by-step breakdown"""
        if not notebook_docs or not self.ai_analyzer or not self.ai_analyzer.enabled:
            return None

        try:
            # Combine notebook content
            combined_content = "\n\n".join([doc.get('content', '') for doc in notebook_docs])

            # Enhanced AI prompt for EXHAUSTIVE detail
            prompt = f"""
Analyze this Databricks notebook in EXHAUSTIVE detail and extract comprehensive transformation logic.

Notebook Path: {notebook_path}

Notebook Code:
{combined_content[:100000]}

Provide a COMPREHENSIVE analysis:

1. **Purpose**: What is the main business purpose of this notebook? (2-3 sentences)

2. **Step-by-Step Logic** (10-30 DETAILED steps):
   - For each data transformation, describe EXACTLY what happens
   - Include table names, column operations, filters, joins, aggregations
   - Example: "Read patient_accounts Delta table from ADLS path /data/bronze/patients"
   - Example: "Filter records where status_code IN ('ACTIVE', 'PENDING') and process_date >= current_date - 30"
   - Example: "Join with permid_lookup on account_number using left outer join"
   - Example: "Create derived column hospital_fk by splitting composite_id on '_' and taking first element"
   - Example: "Aggregate by hospital_fk, count distinct patient_ids, sum total_charges"
   - Example: "Write results to Delta table silver.patient_summary with overwrite mode"
   - Be EXHAUSTIVE - aim for 10-30 steps

3. **Input datasets**: All tables/files being read (Delta, Parquet, CSV, SQL tables with paths)

4. **Output datasets**: All tables/files being written (with write modes: overwrite, append, merge)

5. **Key code snippets** (5-15 snippets):
   - Most important Spark transformation code (5-10 lines each)
   - SQL queries (if using spark.sql)
   - DataFrame operations (filter, join, groupBy, select, withColumn, etc.)
   - Window functions
   - UDFs or complex transformations

Return JSON format:
{{
  "purpose": "Detailed description of notebook purpose...",
  "step_by_step_logic": [
    "Step 1: Read patient_accounts Delta table from ADLS...",
    "Step 2: Filter active records from last 30 days...",
    "Step 3: Join with permid_lookup...",
    ...
  ],
  "inputs": ["delta.`/mnt/adls/bronze/patient_accounts`", "delta.silver.permid_lookup"],
  "outputs": ["delta.silver.patient_summary"],
  "code_snippets": [
    "df_patients = spark.read.format('delta')\\n  .load('/mnt/adls/bronze/patient_accounts')\\n  .filter(col('status_code').isin('ACTIVE', 'PENDING'))",
    "df_joined = df_patients.join(df_permid, on='account_number', how='left')\\n  .withColumn('hospital_fk', split(col('composite_id'), '_')[0])",
    ...
  ],
  "transformations": [
    "Filter active records",
    "Left join with permid lookup",
    "Create hospital_fk derived column",
    ...
  ]
}}

CRITICAL: Provide EXHAUSTIVE detail. Aim for 10-30 step-by-step logic items and 5-15 code snippets.
"""

            # Get AI response with larger content window
            ai_response = self.ai_analyzer.analyze_code(
                code=combined_content[:100000],  # Increased from 50000 to 100000 for complete notebook analysis
                context=prompt,
                analysis_type="deep_notebook_analysis"
            )

            # Parse JSON response
            import json
            try:
                # Try to extract JSON from response
                json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', ai_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                    else:
                        logger.warning("   No JSON found in AI response")
                        return None

                enriched_info = json.loads(json_str)
                step_count = len(enriched_info.get('step_by_step_logic', []))
                snippet_count = len(enriched_info.get('code_snippets', []))
                logger.info(f"   ✅ AI extracted {step_count} steps, {snippet_count} snippets for {notebook_path}")
                return enriched_info

            except json.JSONDecodeError as e:
                logger.warning(f"   Failed to parse AI response as JSON: {e}")
                return None

        except Exception as e:
            logger.error(f"   ❌ Failed to extract notebook logic with AI: {e}")
            return None

    def _search_notebook_code(self, notebook_path: str) -> List[str]:
        """Fallback: Search for notebook code using basic extraction"""
        if not self.indexer:
            return []

        try:
            # Search for notebook
            search_results = self.indexer.search_multi_collection(
                query=f"Databricks notebook {notebook_path} code transformation",
                collections=["databricks_collection"],
                top_k=20  # Increased from 5 to 20 to ensure we get all notebook chunks
            )

            notebook_docs = search_results.get('databricks_collection', [])

            code_snippets = []
            for doc in notebook_docs:
                content = doc.get('content', '')

                # Extract code snippets (look for function definitions, SQL queries, etc.)
                # Extract first few meaningful lines
                lines = content.split('\n')
                meaningful_lines = [line for line in lines if line.strip() and not line.strip().startswith('#')]

                if meaningful_lines:
                    code_snippets.append('\n'.join(meaningful_lines[:10]))  # First 10 lines

            return code_snippets[:3]  # Return top 3 snippets

        except Exception as e:
            logger.error(f"Failed to search notebook code: {e}")
            return []

    def _extract_activities_with_ai(self, pipeline_docs: List[Dict[str, Any]], pipeline_name: str) -> List[Dict[str, Any]]:
        """Extract activities using AI analysis (AI PRIMARY)"""
        if not pipeline_docs:
            logger.warning("   No pipeline documents to analyze")
            return []

        # AI PRIMARY: Try AI first
        if self.ai_analyzer and self.ai_analyzer.enabled:
            try:
                # Combine document content
                combined_content = "\n\n".join([
                    f"Document: {doc.get('metadata', {}).get('file_name', 'Unknown')}\n{doc.get('content', '')}"
                    for doc in pipeline_docs
                ])

                # AI prompt
                prompt = f"""
Analyze this Databricks/ADF pipeline and extract activity information.

Pipeline Name: {pipeline_name}

Pipeline Content:
{combined_content[:20000]}

Extract the following for each activity:
1. Activity name
2. Notebook path (if applicable)
3. Purpose/description
4. Input datasets
5. Output datasets
6. Transformations performed

Return a structured JSON array with this format:
[
  {{
    "name": "Activity_Name",
    "notebook": "notebook/path or No notebook",
    "purpose": "Purpose description",
    "code_snippets": [],
    "inputs": ["input1"],
    "outputs": ["output1"],
    "transformations": ["Transformation description"]
  }}
]
"""

                # Use analyze_code method
                ai_response = self.ai_analyzer.analyze_code(
                    code=combined_content[:20000],
                    context=f"Databricks pipeline: {pipeline_name}",
                    analysis_type="workflow_extraction"
                )

                activities = self._parse_ai_response_to_activities(ai_response)

                if activities:
                    logger.info(f"   ✅ AI extracted {len(activities)} activities")
                    return activities
                else:
                    logger.warning("   AI returned empty activity list")

            except Exception as e:
                logger.error(f"   ❌ AI extraction failed: {e}")

        # FALLBACK: Return empty if AI fails
        logger.warning("   ⚠ AI extraction unavailable or failed - returning empty")
        return []

    def _build_orchestration_flow_from_activities(self, activities: List[Dict[str, Any]]) -> str:
        """Build orchestration flow from activities"""
        if not activities:
            return "Empty pipeline"

        activity_names = [a.get('name', 'Unknown') for a in activities]

        if len(activity_names) <= 5:
            return " → ".join(activity_names)
        else:
            return f"{' → '.join(activity_names[:3])} → ... → {activity_names[-1]} ({len(activity_names)} activities)"

    def _parse_ai_response_to_activities(self, ai_response: str) -> List[Dict[str, Any]]:
        """Parse AI response into activity structures"""
        try:
            # Try to extract JSON array from response
            json_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r'\[.*\]', ai_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    return []

            activities = json.loads(json_str)

            if isinstance(activities, list):
                return activities
            else:
                return []

        except Exception as e:
            logger.warning(f"Failed to parse AI response as JSON: {e}")
            return []

    def _create_empty_result(self, pipeline_name: str) -> Dict[str, Any]:
        """Create empty result when no pipeline found"""
        return {
            'pipeline_name': pipeline_name,
            'system': 'databricks',
            'activities': [],
            'orchestration_flow': '',
            'conditional_branches': {},
            'total_activities': 0
        }


# Example usage
if __name__ == "__main__":
    # Test without indexer/AI
    extractor = DatabricksLogicExtractor()

    print("\n" + "=" * 80)
    print("DATABRICKS LOGIC EXTRACTOR TEST")
    print("=" * 80)

    test_pipeline = "pl_cdd_bdf_download"

    print(f"\nTesting pipeline: {test_pipeline}")
    print("Note: Without indexer/AI, this will return empty result")

    result = extractor.extract_logic(test_pipeline)

    print(f"\n✅ Extraction Result:")
    print(f"   Pipeline: {result['pipeline_name']}")
    print(f"   System: {result['system']}")
    print(f"   Total Activities: {result['total_activities']}")
    print(f"   Orchestration Flow: {result['orchestration_flow']}")
    print(f"   Conditional Branches: {len(result['conditional_branches'])}")

    print("\n" + "=" * 80)
    print("Note: Full functionality requires indexer and AI analyzer")
    print("=" * 80)
