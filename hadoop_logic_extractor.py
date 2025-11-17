"""
Enhanced Hadoop Logic Extractor

Performs DEEP script-by-script analysis for comprehensive migration comparison.

Key Enhancements:
1. Finds ALL scripts referenced in workflow (not just top 20 docs)
2. Analyzes each script individually with full content
3. Extracts step-by-step logic (10-30 steps per script)
4. Captures code snippets (5-15 key sections per script)
5. Detailed transformations with actual column names

This matches the manual comparison format with 50-70 rows of detailed analysis.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
from loguru import logger
import re
import json


class HadoopLogicExtractor:
    """Enhanced Hadoop Logic Extractor with deep script-by-script analysis"""

    def __init__(self, indexer=None, ai_analyzer=None):
        """
        Initialize Enhanced Hadoop Logic Extractor

        Args:
            indexer: MultiCollectionIndexer for vector search
            ai_analyzer: AIAnalyzer for natural language extraction
        """
        self.indexer = indexer
        self.ai_analyzer = ai_analyzer

    def extract_logic(self, workflow_name: str) -> Dict[str, Any]:
        """
        Extract comprehensive logic with DEEP script-by-script analysis

        Args:
            workflow_name: Name of the Hadoop workflow

        Returns:
            {
                'workflow_name': str,
                'system': 'hadoop',
                'jobs': [
                    {
                        'name': str,
                        'script_file': str,
                        'purpose': str,
                        'step_by_step_logic': List[str],  # 10-30 detailed steps
                        'code_snippets': List[Dict],      # 5-15 key code sections
                        'inputs': List[str],
                        'outputs': List[str],
                        'transformations': List[str],
                        'dependencies': List[str]
                    }
                ],
                'oozie_flow': str,
                'data_lineage': Dict[str, Any],
                'total_jobs': int,
                'total_scripts': int
            }
        """
        logger.info(f"📊 Extracting Hadoop logic (DEEP MODE): {workflow_name}")

        # Step 1: Find ALL scripts in workflow
        scripts = self._find_all_scripts(workflow_name)

        if not scripts:
            logger.warning(f"   ⚠ No scripts found for {workflow_name} - using fallback")
            return self._extract_logic_fallback(workflow_name)

        logger.info(f"   Found {len(scripts)} scripts to analyze")

        # Step 2: Deeply analyze each script
        jobs = []
        for i, script_name in enumerate(scripts, 1):
            logger.info(f"   [{i}/{len(scripts)}] Analyzing: {script_name}")

            script_analysis = self._analyze_script_deeply(script_name, workflow_name)

            if script_analysis:
                jobs.append(script_analysis)
            else:
                # If deep analysis fails, add placeholder
                jobs.append({
                    'name': script_name,
                    'script_file': script_name,
                    'purpose': f"Execute {script_name}",
                    'step_by_step_logic': [f"1. Run {script_name}"],
                    'code_snippets': [],
                    'inputs': [],
                    'outputs': [],
                    'transformations': [],
                    'dependencies': []
                })

        # Step 3: Extract Oozie flow
        oozie_flow = self._extract_oozie_flow(workflow_name)

        # Step 4: Build data lineage
        data_lineage = self._build_data_lineage(jobs)

        result = {
            'workflow_name': workflow_name,
            'system': 'hadoop',
            'jobs': jobs,
            'oozie_flow': oozie_flow,
            'data_lineage': data_lineage,
            'total_jobs': len(jobs),
            'total_scripts': len(scripts)
        }

        logger.info(f"✅ Hadoop logic extraction complete: {len(jobs)} jobs from {len(scripts)} scripts")
        return result

    def _find_all_scripts(self, workflow_name: str) -> List[str]:
        """
        Find ALL script files referenced in workflow - ENHANCED to find shell/python scripts

        Handles multiple Oozie XML patterns:
        - Pig scripts: <script>merge_swift.pig</script>
        - Shell scripts (simple): <exec>get_datetime.sh</exec>
        - Shell scripts (bash -c): <exec>bash</exec><argument>${appPath}/get_bdf.sh</argument>
        - Python/Spark scripts: <jar>${appPath}/log_notification.py</jar>

        Returns:
            List of script file names (e.g., ['get_bdf.sh', 'process_bdf.sh', 'merge_swift.pig', ...])
        """
        if not self.indexer:
            logger.warning("   No indexer - cannot find scripts")
            return []

        try:
            # Search for workflow definition (workflow.xml or coordinator.xml)
            search_results = self.indexer.search_multi_collection(
                query=f"{workflow_name} workflow.xml coordinator oozie action",
                collections=["hadoop_collection"],
                top_k=10
            )

            workflow_docs = search_results.get('hadoop_collection', [])

            if not workflow_docs:
                logger.warning(f"   No workflow definition found for {workflow_name}")
                # Fallback: Try direct file system search
                return self._scan_script_directory(workflow_name)

            # Extract script references from ALL workflow documents
            scripts = set()  # Use set to avoid duplicates
            for doc in workflow_docs:
                content = doc.get('content', '')

                # Pattern 1: Pig <script> tags (most common)
                # Example: <script>merge_swift.pig</script>
                script_matches = re.findall(r'<script[^>]*>([^<]+)</script>', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in script_matches if s.strip()])

                # Pattern 2: Shell <exec> tags (simple form)
                # Example: <exec>get_datetime.sh</exec>
                exec_matches = re.findall(r'<exec>([^<]+\.sh)</exec>', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in exec_matches])

                # Pattern 3: Shell scripts via bash -c with <argument> tags
                # Example: <exec>bash</exec><argument>-c</argument><argument>${appPath}/get_bdf.sh</argument>
                bash_argument_pattern = r'<exec>(?:bash|/bin/bash)</exec>.*?<argument>-c</argument>.*?<argument>([^<]+)</argument>'
                bash_matches = re.findall(bash_argument_pattern, content, re.DOTALL | re.IGNORECASE)
                for match in bash_matches:
                    # Extract script name from ${appPath}/script.sh or full path
                    script_path = match.replace('${appPath}/', '').strip()
                    # Split by space to handle arguments like "audit_bdf_swift.sh ${date} es_swift"
                    script_name = script_path.split()[0]
                    if script_name.endswith('.sh'):
                        scripts.add(script_name)

                # Pattern 4: Python/Spark scripts in <jar> tags
                # Example: <jar>${appPath}/log_notification.py</jar>
                jar_python_pattern = r'<jar>([^<]+\.py)</jar>'
                python_matches = re.findall(jar_python_pattern, content, re.IGNORECASE)
                for match in python_matches:
                    script_name = match.replace('${appPath}/', '').strip()
                    scripts.add(script_name)

                # Pattern 5: pig -f command line
                pig_matches = re.findall(r'pig\s+-f\s+([^\s]+\.pig)', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in pig_matches])

                # Pattern 6: hive -f script.hql
                hive_matches = re.findall(r'hive\s+-f\s+([^\s]+\.hql)', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in hive_matches])

                # Pattern 7: python command line
                python_matches = re.findall(r'python\s+([^\s]+\.py)', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in python_matches])

            # Fallback: If we found very few scripts, scan file system directly
            if len(scripts) < 5:
                logger.warning(f"   ⚠ Only {len(scripts)} scripts found via patterns - trying direct scan")
                filesystem_scripts = self._scan_script_directory(workflow_name)
                scripts.update(filesystem_scripts)

            # Convert to sorted list
            unique_scripts = sorted([s for s in scripts if s])

            logger.info(f"   ✅ Found {len(unique_scripts)} unique scripts in {workflow_name}")
            for script in unique_scripts:
                logger.info(f"      - {script}")
            return unique_scripts

        except Exception as e:
            logger.error(f"   ❌ Failed to find scripts: {e}")
            # Fallback to direct scan
            return self._scan_script_directory(workflow_name)

    def _scan_script_directory(self, workflow_name: str) -> List[str]:
        """
        Fallback method: Scan file system directly for scripts in workflow directory

        This is used when:
        1. Vector DB search fails to find workflow.xml
        2. XML pattern matching finds very few scripts
        3. Workflow XML has non-standard script references

        Scans: hadoop_repos/hadoop_repos/app-cdd/oozie/{workflow_name}/
        """
        scripts = set()

        # Clean workflow name (remove "cdd: " prefix if present)
        clean_workflow = workflow_name.replace('cdd: ', '').replace('cdd:', '').strip()

        # Possible base paths
        base_paths = [
            f"/Users/ankurshome/Desktop/Hadoop_Parser/CodebaseIntelligence/hadoop_repos/hadoop_repos/app-cdd/oozie/{clean_workflow}",
            f"/Users/ankurshome/Desktop/Hadoop_Parser/CodebaseIntelligence/hadoop_repos/hadoop_repos/app-cdd/oozie/{workflow_name}",
            # Also check parent directories for common scripts
            f"/Users/ankurshome/Desktop/Hadoop_Parser/CodebaseIntelligence/hadoop_repos/hadoop_repos/app-cdd/pig/es",
            f"/Users/ankurshome/Desktop/Hadoop_Parser/CodebaseIntelligence/hadoop_repos/hadoop_repos/app-cdd/pig/ie",
        ]

        import glob
        from pathlib import Path

        for base_path in base_paths:
            if not Path(base_path).exists():
                continue

            logger.info(f"      🔍 Scanning directory: {base_path}")

            # Find all script files
            for ext in ['*.pig', '*.sh', '*.py', '*.hql', '*.sql']:
                pattern = f"{base_path}/{ext}"
                matches = glob.glob(pattern)

                for match in matches:
                    script_name = Path(match).name
                    scripts.add(script_name)
                    logger.info(f"         Found: {script_name}")

            # Also check subdirectories (1 level deep)
            for ext in ['*.pig', '*.sh', '*.py', '*.hql', '*.sql']:
                pattern = f"{base_path}/*/{ext}"
                matches = glob.glob(pattern)

                for match in matches:
                    script_name = Path(match).name
                    scripts.add(script_name)
                    logger.info(f"         Found: {script_name}")

        if scripts:
            logger.info(f"      ✅ File system scan found {len(scripts)} scripts")
        else:
            logger.warning(f"      ⚠ File system scan found no scripts in expected locations")

        return sorted(list(scripts))

    def _analyze_script_deeply(self, script_name: str, workflow_name: str) -> Optional[Dict[str, Any]]:
        """
        Perform DEEP AI-powered analysis of a single script

        Returns:
            {
                'name': str,
                'script_file': str,
                'purpose': str,
                'step_by_step_logic': List[str],  # 10-30 detailed numbered steps
                'code_snippets': List[Dict],      # 5-15 key code sections
                'inputs': List[str],
                'outputs': List[str],
                'transformations': List[str],
                'dependencies': List[str]
            }
        """
        if not self.indexer:
            logger.warning(f"   No indexer - cannot analyze {script_name}")
            return None

        # Search for script file content
        search_results = self.indexer.search_multi_collection(
            query=f"{script_name} {workflow_name} script code content",
            collections=["hadoop_collection"],
            top_k=25  # Increased from 15 to 25 to ensure we get all script chunks
        )

        hadoop_docs = search_results.get('hadoop_collection', [])

        # Find the actual script file
        script_content = None
        script_path = None
        for doc in hadoop_docs:
            file_name = doc.get('metadata', {}).get('file_name', '')
            if script_name.lower() in file_name.lower():
                script_content = doc.get('content', '')
                script_path = doc.get('metadata', {}).get('file_path', '')
                logger.info(f"      📄 Found script: {script_path}")
                break

        if not script_content:
            logger.warning(f"      ⚠ Script content not found: {script_name}")
            return self._create_placeholder_job(script_name)

        # DEEP AI ANALYSIS
        if not self.ai_analyzer or not self.ai_analyzer.enabled:
            logger.warning("      AI analyzer not available - using pattern matching")
            return self._analyze_script_with_patterns(script_name, script_content)

        # Construct comprehensive AI prompt
        prompt = f"""You are analyzing a Hadoop script in EXTREME DETAIL for migration comparison.

CONTEXT:
- Workflow: {workflow_name}
- Script: {script_name}
- System: Hadoop
- Purpose: Generate detailed analysis matching manual comparison format

FULL SCRIPT CONTENT:
{script_content[:100000]}

TASK: Provide COMPREHENSIVE DEEP ANALYSIS with MAXIMUM DETAIL

Extract the following:

1. **Purpose** (1-2 sentences): What business function does this script perform?

2. **Step-by-Step Logic** (Numbered list, aim for 10-30 steps):
   - Break down the script line-by-line or operation-by-operation
   - For EACH significant operation, describe what it does
   - Reference actual code lines, variables, or commands
   - Examples:
     • "1. Assign first command-line argument to 'date' variable: `date=$1`"
     • "2. Check if USER variable is empty, if so assign from $1: `[ -z $USER ] && USER=$1`"
     • "3. Securely copy BDF ZIP files from remote server using scp command"
     • "4. Load input data from /data/raw/bdf/*.dat using Pig LOAD statement"
     • "5. Filter records where status='ACTIVE' and date >= current_date"

3. **Code Snippets** (5-15 key code sections):
   - Extract the MOST IMPORTANT code sections that show transformations/logic
   - Each snippet should have:
     • line_range: "Lines 10-15" or "Line 42" (approximate if unknown)
     • code: Actual code (5-20 lines, verbatim from script)
     • purpose: What this code section does (1 sentence)
   - Focus on: data loading, transformations, filtering, joins, aggregations, outputs
   - Examples:
     {{
       "line_range": "Lines 25-30",
       "code": "patients = LOAD '/data/patients.dat' USING PigStorage(',')\\n  AS (id:int, name:chararray, dob:chararray);\\nfiltered = FILTER patients BY dob IS NOT NULL;",
       "purpose": "Load patient data and filter out records with null DOB"
     }}

4. **Input Data** (List ALL input sources):
   - File paths, table names, external systems, HDFS paths
   - Example: ["/data/raw/bdf/*.zip", "patients_table", "SFTP://server/files"]

5. **Output Data** (List ALL output destinations):
   - Output files, tables, databases, HDFS paths
   - Example: ["/data/processed/bdf_merged.dat", "bdf_results_table"]

6. **Transformations** (List ALL data transformations with SPECIFICS):
   - Filters: "Filter records where age > 18 AND status='ACTIVE'"
   - Joins: "Join patients with accounts on patient_id (LEFT OUTER JOIN)"
   - Aggregations: "Group by (hospital_id, date), count records, sum(amount)"
   - Calculations: "Calculate total_amount = quantity * unit_price"
   - Data type conversions: "Cast date_string to DATE format"
   - Be SPECIFIC with column names, conditions, and formulas

7. **Dependencies** (List external resources):
   - JAR files, UDF libraries, configuration files, Python modules
   - Example: ["piggybank.jar", "config.properties", "dateValidation.py", "custom_udfs.jar"]

RETURN AS JSON (CRITICAL - MUST BE VALID JSON):
{{
  "name": "{script_name}",
  "script_file": "{script_path or script_name}",
  "purpose": "Brief 1-2 sentence purpose describing business function",
  "step_by_step_logic": [
    "1. First step with code reference or command",
    "2. Second step with specific details",
    "3. Load input data from specific path/table",
    "4. Apply transformation: specific filter condition",
    "..."
  ],
  "code_snippets": [
    {{
      "line_range": "Lines 10-15",
      "code": "actual code here (5-20 lines verbatim)",
      "purpose": "What this code section does"
    }},
    {{
      "line_range": "Lines 42-50",
      "code": "another code section",
      "purpose": "Purpose of this section"
    }}
  ],
  "inputs": ["input_file1.dat", "/hdfs/path/to/data"],
  "outputs": ["output_file.dat", "result_table"],
  "transformations": [
    "Filter records where column='value' AND condition",
    "Join table1 with table2 on join_key (INNER JOIN)",
    "Group by (col1, col2) and aggregate: COUNT(*), SUM(amount)",
    "Calculate new_column = expression"
  ],
  "dependencies": ["library.jar", "config.properties", "python_module.py"]
}}

CRITICAL REQUIREMENTS:
- Be EXHAUSTIVE. Aim for 10-30 step-by-step logic items (not 3-5)
- Include 5-15 code snippets showing actual code
- Be SPECIFIC with column names, file paths, conditions
- This will be used for migration validation - DETAIL is critical
"""

        try:
            # Get AI analysis with larger content window
            ai_response = self.ai_analyzer.analyze_code(
                code=script_content[:100000],  # Increased from 50000 to 100000 for complete script analysis
                context=prompt,
                analysis_type="deep_script_analysis"
            )

            # Parse JSON response
            script_analysis = self._parse_ai_response_to_script(ai_response)

            if script_analysis and script_analysis.get('step_by_step_logic'):
                # CRITICAL: Always extract outputs from script content (don't rely solely on AI)
                # This ensures STTM has real table names for comparison
                actual_outputs = self._extract_outputs_from_content(script_content)

                # If AI didn't provide outputs or provided fewer than actual, use actual
                ai_outputs = script_analysis.get('outputs', [])
                if not ai_outputs or len(actual_outputs) > len(ai_outputs):
                    logger.info(f"      ✅ Overriding AI outputs with actual: {actual_outputs}")
                    script_analysis['outputs'] = actual_outputs
                else:
                    # Merge both (AI might have found additional context)
                    combined_outputs = list(set(ai_outputs + actual_outputs))
                    script_analysis['outputs'] = combined_outputs

                # Same for inputs
                actual_inputs = self._extract_inputs_from_content(script_content)
                ai_inputs = script_analysis.get('inputs', [])
                if not ai_inputs or len(actual_inputs) > len(ai_inputs):
                    logger.info(f"      ✅ Overriding AI inputs with actual: {actual_inputs}")
                    script_analysis['inputs'] = actual_inputs
                else:
                    combined_inputs = list(set(ai_inputs + actual_inputs))
                    script_analysis['inputs'] = combined_inputs

                logger.info(f"      ✅ Deep analysis complete: {len(script_analysis.get('step_by_step_logic', []))} steps, {len(script_analysis.get('code_snippets', []))} snippets")
                logger.info(f"      📊 Tables: {len(script_analysis.get('inputs', []))} inputs, {len(script_analysis.get('outputs', []))} outputs")
                return script_analysis
            else:
                logger.warning(f"      ⚠ AI analysis incomplete for {script_name}")

        except Exception as e:
            logger.error(f"      ❌ AI analysis failed for {script_name}: {e}")

        # Fallback to pattern matching
        return self._analyze_script_with_patterns(script_name, script_content)

    def _parse_ai_response_to_script(self, ai_response: str) -> Optional[Dict[str, Any]]:
        """Parse AI response into script analysis structure with robust error handling"""
        import json

        try:
            # Strategy 1: Extract JSON from markdown code blocks
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Strategy 2: Try to find JSON object directly (look for opening brace to closing brace)
                json_match = re.search(r'\{[^{}]*"name"[^{}]*\{.*\}.*\}', ai_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    # Strategy 3: Find any JSON-like structure
                    start_idx = ai_response.find('{')
                    if start_idx == -1:
                        logger.warning("   No JSON object found in AI response")
                        return None

                    # Find matching closing brace
                    brace_count = 0
                    end_idx = start_idx
                    for i in range(start_idx, len(ai_response)):
                        if ai_response[i] == '{':
                            brace_count += 1
                        elif ai_response[i] == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                end_idx = i + 1
                                break

                    json_str = ai_response[start_idx:end_idx]

            # Try to parse JSON
            try:
                script_analysis = json.loads(json_str)
            except json.JSONDecodeError as e:
                logger.warning(f"   Failed to parse AI JSON: {e}")
                logger.debug(f"   JSON string (first 500 chars): {json_str[:500]}")

                # Try to fix common JSON issues
                # Issue 1: Trailing commas
                json_str_fixed = re.sub(r',(\s*[}\]])', r'\1', json_str)

                # Issue 2: Unescaped quotes in strings
                # (This is complex, skip for now)

                try:
                    script_analysis = json.loads(json_str_fixed)
                    logger.info("   ✅ Fixed JSON parsing after removing trailing commas")
                except:
                    logger.error("   ❌ Could not fix JSON, giving up")
                    return None

            # Validate and normalize required fields
            if 'name' not in script_analysis:
                script_analysis['name'] = 'Unknown Script'

            if 'purpose' not in script_analysis:
                script_analysis['purpose'] = 'No purpose provided'

            if 'step_by_step_logic' not in script_analysis:
                # Try alternate field names
                if 'steps' in script_analysis:
                    script_analysis['step_by_step_logic'] = script_analysis['steps']
                elif 'logic' in script_analysis:
                    script_analysis['step_by_step_logic'] = script_analysis['logic']
                else:
                    script_analysis['step_by_step_logic'] = []

            # Ensure step_by_step_logic is a list
            if not isinstance(script_analysis['step_by_step_logic'], list):
                script_analysis['step_by_step_logic'] = [str(script_analysis['step_by_step_logic'])]

            # Ensure code_snippets exists
            if 'code_snippets' not in script_analysis:
                script_analysis['code_snippets'] = []

            return script_analysis

        except Exception as e:
            logger.error(f"   Error parsing AI response: {e}")
            return None

    def _analyze_script_with_patterns(self, script_name: str, script_content: str) -> Dict[str, Any]:
        """Fallback analysis using pattern matching when AI unavailable"""
        logger.info(f"      Using pattern matching for {script_name}")

        # Extract inputs and outputs
        inputs = self._extract_inputs_from_content(script_content)
        outputs = self._extract_outputs_from_content(script_content)

        # Basic step extraction (split by line, filter comments)
        lines = [line.strip() for line in script_content.split('\n')
                 if line.strip() and not line.strip().startswith('#')]

        # Create step-by-step logic from first 20 significant lines
        step_by_step_logic = [f"{i+1}. {line[:100]}" for i, line in enumerate(lines[:20])]

        return {
            'name': script_name,
            'script_file': script_name,
            'purpose': f"Execute {script_name} (AI unavailable - pattern matching used)",
            'step_by_step_logic': step_by_step_logic if step_by_step_logic else ["1. Execute script"],
            'code_snippets': [],
            'inputs': inputs,
            'outputs': outputs,
            'transformations': ["Pattern matching - detailed analysis unavailable"],
            'dependencies': []
        }

    def _create_placeholder_job(self, script_name: str) -> Dict[str, Any]:
        """Create placeholder job when script content not found"""
        return {
            'name': script_name,
            'script_file': script_name,
            'purpose': f"Execute {script_name} (content not indexed)",
            'step_by_step_logic': [f"1. Run {script_name}"],
            'code_snippets': [],
            'inputs': [],
            'outputs': [],
            'transformations': [],
            'dependencies': []
        }

    def _extract_oozie_flow(self, workflow_name: str) -> str:
        """Extract Oozie workflow orchestration flow"""
        if not self.indexer:
            return "Workflow sequence not available"

        try:
            search_results = self.indexer.search_multi_collection(
                query=f"{workflow_name} workflow.xml oozie",
                collections=["hadoop_collection"],
                top_k=5
            )

            for doc in search_results.get('hadoop_collection', []):
                content = doc.get('content', '')
                metadata = doc.get('metadata', {})

                # Look for Oozie workflow.xml patterns
                if 'workflow.xml' in metadata.get('file_name', '').lower() or '<workflow' in content:
                    # Extract action sequence
                    action_matches = re.findall(r'<action[^>]*name="([^"]+)"', content)
                    if action_matches:
                        return " → ".join(action_matches)

        except Exception as e:
            logger.error(f"   Failed to extract Oozie flow: {e}")

        return "Workflow sequence not found"

    def _build_data_lineage(self, jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build data lineage from jobs"""
        all_inputs = []
        all_outputs = []

        for job in jobs:
            all_inputs.extend(job.get('inputs', []))
            all_outputs.extend(job.get('outputs', []))

        return {
            'inputs': list(set(all_inputs)),
            'outputs': list(set(all_outputs)),
            'intermediate': list(set(all_outputs) & set(all_inputs))  # Files that are both input and output
        }

    def _extract_inputs_from_content(self, content: str) -> List[str]:
        """
        Extract input file/table names from content with enhanced Pig LOAD parsing

        Handles patterns like:
        - LOAD '$inputDir/data.dat' USING ...
        - LOAD '/hdfs/path/to/table' USING ...
        - df = spark.read.format("delta").load("/path/to/table")
        """
        inputs = []

        # Pattern 1: Pig LOAD statements
        # Matches: LOAD 'path' or LOAD '$var/path'
        pig_load_pattern = r'LOAD\s+[\'"]([^\'"]+)[\'"]'
        pig_matches = re.findall(pig_load_pattern, content, re.IGNORECASE)

        for match in pig_matches:
            # Extract table name from path
            # Example: '$inputPostBDFDir/*_cbeMatchAppend.dat' -> 'inputPostBDFDir' or extract from path
            # Example: '/hdfs/user/cdd/input/bdf/data.dat' -> 'bdf' or 'data'

            # Remove variables like $inputDir, $bcdate
            cleaned = re.sub(r'\$\w+', '', match)

            # Split by / and find meaningful table/file names
            parts = [p.strip() for p in cleaned.split('/') if p.strip()]

            # Look for table name (last meaningful part before file extension)
            for part in reversed(parts):
                # Remove file extension and wildcards
                part_cleaned = re.sub(r'\*', '', part)
                part_cleaned = re.sub(r'\.(dat|txt|csv|parquet|json)$', '', part_cleaned, flags=re.IGNORECASE)

                # Skip if empty, too short, or looks like a date
                if part_cleaned and len(part_cleaned) > 2 and not re.match(r'^\d{6,8}$', part_cleaned):
                    inputs.append(part_cleaned)
                    break

        # Pattern 2: Databricks/Spark read statements
        spark_read_patterns = [
            r'\.load\([\'"]([^\'"]+)[\'"]\)',
            r'\.table\([\'"]([^\'"]+)[\'"]\)',
            r'spark\.read\.[^(]+\([\'"]([^\'"]+)[\'"]\)',
        ]

        for pattern in spark_read_patterns:
            spark_matches = re.findall(pattern, content, re.IGNORECASE)
            for match in spark_matches:
                cleaned = re.sub(r'\$\w+', '', match)
                parts = [p.strip() for p in cleaned.split('/') if p.strip()]
                for part in reversed(parts):
                    part_cleaned = re.sub(r'\.(dat|txt|csv|parquet|json|delta)$', '', part, flags=re.IGNORECASE)
                    if part_cleaned and len(part_cleaned) > 2 and not re.match(r'^\d{6,8}$', part_cleaned):
                        inputs.append(part_cleaned)
                        break

        # Pattern 3: Generic INPUT parameters
        generic_patterns = [
            r'INPUT\s*=\s*[\'"]([^\'"]+)[\'"]',
            r'--input[=\s]+([^\s]+)',
            r'scp\s+[^\s]+:([^\s]+)',  # SCP source
        ]

        for pattern in generic_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                cleaned = re.sub(r'\$\w+', '', match)
                parts = [p.strip() for p in cleaned.split('/') if p.strip()]
                for part in reversed(parts):
                    if part and len(part) > 2:
                        inputs.append(part)
                        break

        # Remove duplicates and return
        unique_inputs = list(set(inputs))
        return unique_inputs[:15]  # Limit to 15 unique inputs

    def _extract_outputs_from_content(self, content: str) -> List[str]:
        """
        Extract output file/table names from content with enhanced Pig STORE parsing

        Handles patterns like:
        - STORE data INTO '$outputBaseDir/permIdPatientAcctId/$bcdate'
        - STORE data INTO '/hdfs/path/to/table'
        - df.write.format("delta").save("/path/to/table")
        """
        outputs = []

        # Pattern 1: Pig STORE statements (most common in Hadoop)
        # Matches: STORE alias INTO 'path' or '$var/path'
        pig_store_pattern = r'STORE\s+\w+\s+INTO\s+[\'"]([^\'"]+)[\'"]'
        pig_matches = re.findall(pig_store_pattern, content, re.IGNORECASE)

        for match in pig_matches:
            # Extract table name from path
            # Example: '$outputBaseDir/permIdPatientAcctId/$bcdate' -> 'permIdPatientAcctId'
            # Example: '/hdfs/user/cdd/publish/es/nopermid/20230101' -> 'nopermid'

            # Remove variables like $outputBaseDir, $bcdate, $user
            cleaned = re.sub(r'\$\w+', '', match)

            # Split by / and find the meaningful table name (not empty, not just dates)
            parts = [p.strip() for p in cleaned.split('/') if p.strip()]

            # Look for table name (usually the last meaningful part before date variables)
            for part in reversed(parts):
                # Skip if it looks like a date or is very short
                if part and len(part) > 2 and not re.match(r'^\d{6,8}$', part):
                    outputs.append(part)
                    break

        # Pattern 2: Databricks/Spark write statements
        # Matches: .save("/path/to/table") or .saveAsTable("table_name")
        spark_save_patterns = [
            r'\.save\([\'"]([^\'"]+)[\'"]\)',
            r'\.saveAsTable\([\'"]([^\'"]+)[\'"]\)',
        ]

        for pattern in spark_save_patterns:
            spark_matches = re.findall(pattern, content, re.IGNORECASE)
            for match in spark_matches:
                # Extract table name from path
                cleaned = re.sub(r'\$\w+', '', match)
                parts = [p.strip() for p in cleaned.split('/') if p.strip()]
                for part in reversed(parts):
                    if part and len(part) > 2 and not re.match(r'^\d{6,8}$', part):
                        outputs.append(part)
                        break

        # Pattern 3: Generic OUTPUT parameters
        generic_patterns = [
            r'OUTPUT\s*=\s*[\'"]([^\'"]+)[\'"]',
            r'--output[=\s]+([^\s]+)',
        ]

        for pattern in generic_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                cleaned = re.sub(r'\$\w+', '', match)
                parts = [p.strip() for p in cleaned.split('/') if p.strip()]
                for part in reversed(parts):
                    if part and len(part) > 2:
                        outputs.append(part)
                        break

        # Remove duplicates and return
        unique_outputs = list(set(outputs))
        return unique_outputs[:15]  # Limit to 15 unique outputs

    def _extract_logic_fallback(self, workflow_name: str) -> Dict[str, Any]:
        """Fallback extraction when scripts cannot be found"""
        logger.warning(f"   Using fallback extraction for {workflow_name}")

        # Try to get any workflow documents
        if not self.indexer:
            return self._create_empty_result(workflow_name)

        try:
            search_results = self.indexer.search_multi_collection(
                query=f"Hadoop workflow {workflow_name} oozie jobs",
                collections=["hadoop_collection"],
                top_k=30
            )

            workflow_docs = search_results.get('hadoop_collection', [])

            if not workflow_docs:
                return self._create_empty_result(workflow_name)

            # Extract jobs using old method
            jobs = []
            for doc in workflow_docs:
                content = doc.get('content', '')
                file_name = doc.get('metadata', {}).get('file_name', 'Unknown')

                jobs.append({
                    'name': file_name,
                    'script_file': file_name,
                    'purpose': f"Workflow component: {file_name}",
                    'step_by_step_logic': [f"1. Execute {file_name}"],
                    'code_snippets': [],
                    'inputs': self._extract_inputs_from_content(content),
                    'outputs': self._extract_outputs_from_content(content),
                    'transformations': ["Fallback extraction - limited detail"],
                    'dependencies': []
                })

            return {
                'workflow_name': workflow_name,
                'system': 'hadoop',
                'jobs': jobs,
                'oozie_flow': self._extract_oozie_flow(workflow_name),
                'data_lineage': self._build_data_lineage(jobs),
                'total_jobs': len(jobs),
                'total_scripts': 0
            }

        except Exception as e:
            logger.error(f"   Fallback extraction failed: {e}")
            return self._create_empty_result(workflow_name)

    def _create_empty_result(self, workflow_name: str) -> Dict[str, Any]:
        """Create empty result when no workflow found"""
        return {
            'workflow_name': workflow_name,
            'system': 'hadoop',
            'jobs': [],
            'oozie_flow': '',
            'data_lineage': {'inputs': [], 'outputs': [], 'intermediate': []},
            'total_jobs': 0,
            'total_scripts': 0
        }
