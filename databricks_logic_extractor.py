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
        """
        Extract activities from ADF pipeline JSON with RECURSIVE extraction for nested activities

        Handles:
        - Top-level activities
        - Switch cases (branching logic)
        - IfCondition (true/false branches)
        - ForEach loops
        """
        try:
            with open(adf_json_path, 'r') as f:
                adf_data = json.load(f)

            # Navigate to activities array
            adf_activities = adf_data.get('properties', {}).get('activities', [])

            # Recursively extract all activities (including nested ones)
            all_activities = self._extract_activities_recursive(adf_activities, parent_path="")

            logger.info(f"   Extracted {len(all_activities)} total activities from ADF JSON (including nested)")
            return all_activities

        except Exception as e:
            logger.error(f"Failed to extract activities from ADF JSON: {e}")
            return []

    def _extract_activities_recursive(self, activities_list: List[Dict], parent_path: str = "") -> List[Dict[str, Any]]:
        """
        Recursively extract activities, including those nested in Switch, IfCondition, ForEach

        Args:
            activities_list: List of ADF activity dictionaries
            parent_path: Path showing nesting (e.g., "check datasource > es_swift")

        Returns:
            Flat list of all activities with their nested path
        """
        extracted = []

        for activity in activities_list:
            activity_name = activity.get('name', 'Unknown')
            activity_type = activity.get('type', 'Unknown')

            # Build current path
            current_path = f"{parent_path} > {activity_name}" if parent_path else activity_name

            # Extract Databricks notebook info
            notebook_path = self._extract_notebook_path(activity)

            # Extract inputs/outputs
            inputs = self._extract_activity_inputs(activity)
            outputs = self._extract_activity_outputs(activity)

            # Extract transformation description
            transformations = self._extract_transformations_from_activity(activity)

            # Add current activity
            extracted.append({
                'name': activity_name,
                'type': activity_type,
                'notebook': notebook_path,
                'purpose': self._get_activity_purpose(activity_type, activity_name),
                'code_snippets': [],  # Will be enriched later
                'inputs': inputs,
                'outputs': outputs,
                'transformations': transformations,
                'path': current_path  # Add path to show nesting
            })

            # RECURSIVE EXTRACTION for nested activities
            type_properties = activity.get('typeProperties', {})

            # Handle Switch activity (branching logic)
            if activity_type == 'Switch':
                cases = type_properties.get('cases', [])
                logger.info(f"      Found Switch '{activity_name}' with {len(cases)} cases")

                for case in cases:
                    case_value = case.get('value', 'Unknown')
                    case_activities = case.get('activities', [])

                    logger.info(f"        Case '{case_value}': {len(case_activities)} activities")

                    # Recursive call for case activities
                    case_path = f"{current_path} [case: {case_value}]"
                    nested = self._extract_activities_recursive(case_activities, case_path)
                    extracted.extend(nested)

                # Also handle default case if present
                default_activities = type_properties.get('defaultActivities', [])
                if default_activities:
                    logger.info(f"        Default case: {len(default_activities)} activities")
                    default_path = f"{current_path} [default]"
                    nested = self._extract_activities_recursive(default_activities, default_path)
                    extracted.extend(nested)

            # Handle IfCondition activity
            elif activity_type == 'IfCondition':
                if_true = type_properties.get('ifTrueActivities', [])
                if_false = type_properties.get('ifFalseActivities', [])

                logger.info(f"      Found IfCondition '{activity_name}': {len(if_true)} true, {len(if_false)} false")

                if if_true:
                    true_path = f"{current_path} [if true]"
                    nested = self._extract_activities_recursive(if_true, true_path)
                    extracted.extend(nested)

                if if_false:
                    false_path = f"{current_path} [if false]"
                    nested = self._extract_activities_recursive(if_false, false_path)
                    extracted.extend(nested)

            # Handle ForEach activity
            elif activity_type == 'ForEach':
                foreach_activities = type_properties.get('activities', [])

                if foreach_activities:
                    logger.info(f"      Found ForEach '{activity_name}': {len(foreach_activities)} activities")
                    foreach_path = f"{current_path} [foreach]"
                    nested = self._extract_activities_recursive(foreach_activities, foreach_path)
                    extracted.extend(nested)

        return extracted

    def _extract_notebook_path(self, activity: Dict[str, Any]) -> str:
        """
        Extract and evaluate Databricks notebook path from activity

        Handles:
        - Static paths: "/CDD/bdf_download/process_bdf"
        - Dynamic expressions: "@concat(pipeline().parameters.notebookpath,'process_bdf')"
        """
        # Look in typeProperties.notebookPath
        notebook_path = activity.get('typeProperties', {}).get('notebookPath', {})

        if isinstance(notebook_path, dict):
            # It's a dynamic expression
            expression = notebook_path.get('value', 'Unknown')

            # Try to evaluate ADF expression
            evaluated = self._evaluate_adf_expression(expression)
            return evaluated

        elif isinstance(notebook_path, str):
            return notebook_path
        else:
            return 'No notebook'

    def _evaluate_adf_expression(self, expression: str) -> str:
        """
        Evaluate ADF pipeline expressions to get actual notebook path

        Common patterns:
        - @concat(pipeline().parameters.notebookpath,'process_bdf')
          → Evaluates to: /CDD/bdf_download/process_bdf

        - @concat(parameters('notebookBasePath'),'/merge_swift')
          → Evaluates to: /CDD/bdf_download/merge_swift
        """
        if not expression or not isinstance(expression, str):
            return expression

        # Pattern 1: @concat(pipeline().parameters.notebookpath,'script_name')
        # Expected value of notebookpath parameter: /CDD/bdf_download/
        concat_match = re.search(r'@concat\([^,]+,\s*[\'"]([^\'"]+)[\'"]\)', expression)

        if concat_match:
            script_name = concat_match.group(1)

            # Infer base path from pipeline context
            # For bdf_download pipeline, notebookpath = /CDD/bdf_download/
            # This can be made more robust by reading pipeline parameters

            # For now, use heuristic: CDD workflows use /CDD/workflow_name/
            base_path = "/CDD/bdf_download"  # TODO: Make this dynamic based on pipeline name

            result = f"{base_path}/{script_name}"
            logger.info(f"      Evaluated expression: {expression} → {result}")
            return result

        # Pattern 2: @pipeline().parameters.paramName
        # Return as-is with indicator it's a parameter
        if '@pipeline()' in expression or '@parameters(' in expression:
            logger.warning(f"      Unevaluated parameter expression: {expression}")
            return expression

        # If no pattern matched, return original
        return expression

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

    def extract_column_schemas_from_notebook(self, notebook_path: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract column-level schemas from Databricks notebook for STTM generation

        Returns:
            {
                'table_name': [
                    {'name': 'HospitalFk', 'type': 'SHORT', 'order': 1, 'source_line': 115, 'transformation': '...'},
                    ...
                ]
            }
        """
        logger.info(f"   🔍 Extracting column schemas from Databricks notebook: {notebook_path}")

        try:
            # Read notebook content
            if not Path(notebook_path).exists():
                logger.warning(f"      Notebook not found: {notebook_path}")
                return {}

            with open(notebook_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            # Use the PySpark extraction logic from HadoopLogicExtractor
            # Since Databricks notebooks are PySpark, we use the same patterns
            return self._extract_column_schemas_from_pyspark(content, Path(notebook_path).name)

        except Exception as e:
            logger.error(f"      Failed to extract column schemas from notebook: {e}")
            return {}

    def _extract_column_schemas_from_pyspark(self, content: str, notebook_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract column-level schemas from PySpark/Databricks notebook content

        This is the same logic as in HadoopLogicExtractor but duplicated here to avoid circular imports
        """
        import re

        logger.info(f"      Parsing PySpark transformations in {notebook_name}")

        table_schemas = {}
        lines = content.split('\n')
        dataframe_schemas = {}

        # Step 1: Parse .select() operations to track column schemas
        select_pattern = r'(\w+)\s*=\s*(\w+)\.select\('

        for i, line in enumerate(lines):
            select_match = re.search(select_pattern, line, re.IGNORECASE)
            if select_match:
                result_alias = select_match.group(1)
                source_alias = select_match.group(2)

                # Extract full select block (may span multiple lines)
                select_block = self._extract_select_block(lines, i)

                # Parse select fields
                columns = self._parse_pyspark_select_block(select_block, i + 1)

                if columns:
                    dataframe_schemas[result_alias] = columns
                    logger.debug(f"         Found select schema for '{result_alias}': {len(columns)} columns")

        # Step 2: Parse .withColumn() operations
        withcol_pattern = r'(\w+)\s*=\s*(\w+)\.withColumn\([\'"]([^\'\"]+)[\'"]\s*,\s*'

        for i, line in enumerate(lines):
            withcol_match = re.search(withcol_pattern, line, re.IGNORECASE)
            if withcol_match:
                result_alias = withcol_match.group(1)
                source_alias = withcol_match.group(2)
                new_col_name = withcol_match.group(3)

                # Extract transformation (may be on next line)
                transformation = self._extract_transformation(lines, i)

                # Copy source schema and add new column
                source_schema = dataframe_schemas.get(source_alias, [])
                new_schema = [col.copy() for col in source_schema]

                new_schema.append({
                    'name': new_col_name,
                    'type': 'STRING',  # Default type
                    'order': len(new_schema) + 1,
                    'source_line': i + 1,
                    'transformation': transformation
                })

                dataframe_schemas[result_alias] = new_schema
                logger.debug(f"         Found withColumn for '{result_alias}': added {new_col_name}")

        # Step 3: Find write operations and map to table names
        write_patterns = [
            r'writecsv\([^,]+,\s*(\w+)\s*,\s*[^+]*\+[\'"]([^\'"]+)[\'"]\+',
            r'(\w+)\.write.*?\.save\([^+]*\+[\'"]([^\'"]+)[\'"]\)',
            r'(\w+)\.write.*?\.saveAsTable\([\'"]([^\'"]+)[\'"]\)',
        ]

        for i, line in enumerate(lines):
            for pattern in write_patterns:
                write_match = re.search(pattern, line, re.IGNORECASE)
                if write_match:
                    df_alias = write_match.group(1)
                    path_or_table = write_match.group(2)

                    # Extract table name from path
                    table_name = None
                    if '/' in path_or_table:
                        parts = [p.strip() for p in path_or_table.split('/') if p.strip()]
                        for part in reversed(parts):
                            if part and len(part) > 2 and not re.match(r'^\d{6,8}$', part):
                                table_name = part
                                break
                    else:
                        table_name = path_or_table

                    if table_name:
                        schema = dataframe_schemas.get(df_alias, [])
                        if schema:
                            table_schemas[table_name] = schema
                            logger.info(f"      ✅ Extracted schema for table '{table_name}': {len(schema)} columns")
                        else:
                            logger.warning(f"      ⚠ No schema found for DataFrame '{df_alias}' (table: {table_name})")

        logger.info(f"   ✅ Extracted {len(table_schemas)} table schemas from Databricks notebook")
        return table_schemas

    def _extract_select_block(self, lines: List[str], start_line: int) -> str:
        """Extract multi-line select block"""
        block = lines[start_line]
        paren_count = block.count('(') - block.count(')')

        i = start_line + 1
        while i < len(lines) and paren_count > 0:
            block += ' ' + lines[i].strip()
            paren_count += lines[i].count('(') - lines[i].count(')')
            i += 1

        return block

    def _parse_pyspark_select_block(self, select_block: str, source_line: int) -> List[Dict[str, Any]]:
        """Parse PySpark select block to extract columns"""
        import re

        columns = []

        # Extract content between .select( and final )
        select_match = re.search(r'\.select\((.*)\)$', select_block, re.DOTALL)
        if not select_match:
            return columns

        select_content = select_match.group(1)

        # Split by commas (handling nested parentheses)
        parts = []
        current = ""
        paren_depth = 0

        for char in select_content:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                parts.append(current.strip())
                current = ""
                continue

            current += char

        if current.strip():
            parts.append(current.strip())

        # Parse each field
        for idx, part in enumerate(parts):
            # Pattern 1: col("value").substr(1,64).alias("ID")
            # Pattern 2: trim(col("field")).alias("field")
            # Pattern 3: "*" (all columns)

            if part.strip() == '*':
                continue  # Skip wildcard

            alias_match = re.search(r'\.alias\([\'"]([^\'\"]+)[\'"]\)', part, re.IGNORECASE)
            if alias_match:
                col_name = alias_match.group(1)
                transformation = part[:alias_match.start()].strip()
            else:
                # Try to extract from col("name")
                col_match = re.search(r'col\([\'"]([^\'\"]+)[\'"]\)', part)
                if col_match:
                    col_name = col_match.group(1)
                else:
                    col_name = f"field_{idx+1}"

                transformation = part.strip()

            columns.append({
                'name': col_name,
                'type': 'STRING',  # Default type
                'order': idx + 1,
                'source_line': source_line,
                'transformation': transformation
            })

        return columns

    def _extract_transformation(self, lines: List[str], start_line: int) -> str:
        """Extract transformation expression from withColumn"""
        # Start from the withColumn line and extract until closing paren
        transformation = lines[start_line]
        paren_count = transformation.count('(') - transformation.count(')')

        i = start_line + 1
        while i < len(lines) and paren_count > 0:
            transformation += ' ' + lines[i].strip()
            paren_count += lines[i].count('(') - lines[i].count(')')
            i += 1

        # Extract just the transformation part (after column name)
        withcol_match = re.search(r'\.withColumn\([^,]+,\s*(.+)\)\s*$', transformation)
        if withcol_match:
            return withcol_match.group(1).strip()
        else:
            return transformation.strip()

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

                # CRITICAL: Extract column-level schemas for STTM generation
                # Try to get actual file path from first document metadata
                actual_file_path = None
                if notebook_docs:
                    metadata = notebook_docs[0].get('metadata', {})
                    actual_file_path = metadata.get('file_path', '') or metadata.get('source', '')

                if actual_file_path and Path(actual_file_path).exists():
                    column_schemas = self.extract_column_schemas_from_notebook(actual_file_path)
                    if column_schemas:
                        enriched_info['column_schemas'] = column_schemas
                        total_columns = sum(len(cols) for cols in column_schemas.values())
                        logger.info(f"   📋 Extracted {len(column_schemas)} table schemas with {total_columns} total columns")
                    else:
                        enriched_info['column_schemas'] = {}
                else:
                    logger.debug(f"   No file path found for {notebook_path} - skipping column schema extraction")
                    enriched_info['column_schemas'] = {}

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
