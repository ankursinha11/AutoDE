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
        Find ALL script files referenced in workflow

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
                return []

            # Extract script references from ALL workflow documents
            scripts = set()  # Use set to avoid duplicates
            for doc in workflow_docs:
                content = doc.get('content', '')

                # Pattern 1: <script>path/to/script.sh</script>
                script_matches = re.findall(r'<script[^>]*>([^<]+)</script>', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in script_matches if s.strip()])

                # Pattern 2: pig -f script.pig
                pig_matches = re.findall(r'pig\s+-f\s+([^\s]+\.pig)', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in pig_matches])

                # Pattern 3: hive -f script.hql
                hive_matches = re.findall(r'hive\s+-f\s+([^\s]+\.hql)', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in hive_matches])

                # Pattern 4: python script.py
                python_matches = re.findall(r'python\s+([^\s]+\.py)', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in python_matches])

                # Pattern 5: bash/sh script.sh
                shell_matches = re.findall(r'(?:bash|sh)\s+([^\s]+\.sh)', content, re.IGNORECASE)
                scripts.update([s.split('/')[-1].strip() for s in shell_matches])

                # Pattern 6: <name>script_name</name> in action elements
                action_names = re.findall(r'<action[^>]*name="([^"]+)"', content, re.IGNORECASE)
                for action in action_names:
                    # Check if action name looks like a script name
                    if any(ext in action.lower() for ext in ['.sh', '.pig', '.hql', '.py']):
                        scripts.add(action.split('/')[-1].strip())

            # Convert to sorted list
            unique_scripts = sorted([s for s in scripts if s])

            logger.info(f"   ✅ Found {len(unique_scripts)} unique scripts in {workflow_name}")
            return unique_scripts

        except Exception as e:
            logger.error(f"   ❌ Failed to find scripts: {e}")
            return []

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
            top_k=15
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
{script_content[:50000]}

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
            # Get AI analysis
            ai_response = self.ai_analyzer.analyze_code(
                code=script_content[:50000],
                context=prompt,
                analysis_type="deep_script_analysis"
            )

            # Parse JSON response
            script_analysis = self._parse_ai_response_to_script(ai_response)

            if script_analysis and script_analysis.get('step_by_step_logic'):
                logger.info(f"      ✅ Deep analysis complete: {len(script_analysis.get('step_by_step_logic', []))} steps, {len(script_analysis.get('code_snippets', []))} snippets")
                return script_analysis
            else:
                logger.warning(f"      ⚠ AI analysis incomplete for {script_name}")

        except Exception as e:
            logger.error(f"      ❌ AI analysis failed for {script_name}: {e}")

        # Fallback to pattern matching
        return self._analyze_script_with_patterns(script_name, script_content)

    def _parse_ai_response_to_script(self, ai_response: str) -> Optional[Dict[str, Any]]:
        """Parse AI response into script analysis structure"""
        try:
            # Extract JSON from response (handle markdown code blocks)
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find JSON object directly
                json_match = re.search(r'\{.*?"name".*?\}', ai_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    return None

            script_analysis = json.loads(json_str)

            # Validate required fields
            if not all(key in script_analysis for key in ['name', 'purpose', 'step_by_step_logic']):
                logger.warning("   AI response missing required fields")
                return None

            return script_analysis

        except json.JSONDecodeError as e:
            logger.warning(f"   Failed to parse AI JSON: {e}")
            return None
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
        """Extract input file/table names from content"""
        inputs = []

        # Pattern: LOAD, FROM, INPUT, etc.
        load_patterns = [
            r'LOAD\s+[\'"]([^\'"]+)[\'"]',
            r'FROM\s+(\w+)',
            r'INPUT\s*=\s*[\'"]([^\'"]+)[\'"]',
            r'--input[=\s]+([^\s]+)',
            r'scp\s+[^\s]+:([^\s]+)',  # SCP source
        ]

        for pattern in load_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            inputs.extend(matches)

        return list(set(inputs))[:15]  # Limit to 15 unique inputs

    def _extract_outputs_from_content(self, content: str) -> List[str]:
        """Extract output file/table names from content"""
        outputs = []

        # Pattern: STORE, INTO, OUTPUT, etc.
        store_patterns = [
            r'STORE\s+\w+\s+INTO\s+[\'"]([^\'"]+)[\'"]',
            r'INTO\s+(\w+)',
            r'OUTPUT\s*=\s*[\'"]([^\'"]+)[\'"]',
            r'--output[=\s]+([^\s]+)',
        ]

        for pattern in store_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            outputs.extend(matches)

        return list(set(outputs))[:15]  # Limit to 15 unique outputs

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
