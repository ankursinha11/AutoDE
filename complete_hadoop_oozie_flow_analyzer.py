#!/usr/bin/env python3
"""
Complete Hadoop Oozie Flow + Deep Code Analysis
==============================================

This tool combines:
1. Oozie workflow execution order and frequency
2. Which scripts are used in each workflow step
3. Deep code analysis of each script
4. Complete pipeline flow with business logic
5. Used vs unused scripts analysis

All in one comprehensive Excel report.
"""

import os
import re
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd

@dataclass
class OozieWorkflow:
    """Represents an Oozie workflow with execution details"""
    name: str
    file_path: str
    execution_order: int
    frequency: str
    total_actions: int
    actions: List[Dict[str, Any]]
    technologies: Dict[str, int]
    input_tables: List[str]
    output_tables: List[str]
    business_domain: str
    critical_path: List[str]

@dataclass
class WorkflowAction:
    """Represents a single action within a workflow"""
    workflow_name: str
    action_name: str
    action_type: str
    technology: str
    script_path: str
    script_name: str
    execution_order: int
    input_tables: List[str]
    output_tables: List[str]
    business_purpose: str
    transformations: List[Dict[str, Any]]
    joins: List[Dict[str, Any]]
    data_quality_rules: List[Dict[str, Any]]
    business_rules: List[str]
    column_mappings: List[Dict[str, Any]]
    complexity_score: int
    dependencies: List[str]

@dataclass
class CompletePipelineFlow:
    """Complete pipeline flow with all details"""
    workflow_name: str
    workflow_frequency: str
    step_number: int
    action_name: str
    script_name: str
    technology: str
    execution_order: int
    input_tables: str
    output_tables: str
    business_purpose: str
    transformations_count: int
    joins_count: int
    data_quality_rules_count: int
    complexity_score: int
    is_critical_path: bool

class CompleteHadoopAnalyzer:
    """Complete Hadoop analysis combining Oozie flow and deep code analysis"""
    
    def __init__(self, hadoop_repo_path: str):
        self.hadoop_repo_path = Path(hadoop_repo_path)
        self.workflows = []
        self.workflow_actions = []
        self.used_scripts = set()
        self.unused_scripts = set()
        self.table_registry = {}
        
    def analyze_complete_hadoop_repository(self) -> Dict[str, Any]:
        """Perform complete Hadoop repository analysis"""
        print("🚀 Complete Hadoop Oozie Flow + Deep Code Analysis")
        print("=" * 70)
        
        # Step 1: Parse DDL files
        print("🔍 Step 1: Parsing DDL files...")
        self._parse_ddl_files()
        
        # Step 2: Find and analyze Oozie workflows
        print("🔍 Step 2: Analyzing Oozie workflows...")
        oozie_workflows = self._find_oozie_workflows()
        workflow_analyses = self._analyze_workflows_complete(oozie_workflows)
        
        # Step 3: Find all scripts
        print("🔍 Step 3: Finding all scripts...")
        all_scripts = self._find_all_scripts()
        
        # Step 4: Identify used vs unused scripts
        print("🔍 Step 4: Identifying used vs unused scripts...")
        self._identify_used_scripts(all_scripts, workflow_analyses)
        
        # Step 5: Deep analysis of used scripts
        print("🔍 Step 5: Deep analysis of used scripts...")
        self._deep_analyze_used_scripts()
        
        # Step 6: Create complete pipeline flow
        print("🔍 Step 6: Creating complete pipeline flow...")
        complete_pipeline = self._create_complete_pipeline_flow()
        
        # Step 7: Generate comprehensive report
        return self._generate_complete_report(workflow_analyses, complete_pipeline)
    
    def _parse_ddl_files(self):
        """Parse DDL files to understand table schemas"""
        ddl_files = list(self.hadoop_repo_path.glob("**/*.sql"))
        for ddl_file in ddl_files:
            try:
                content = ddl_file.read_text()
                tables = self._extract_table_schemas(content)
                self.table_registry.update(tables)
            except Exception as e:
                print(f"   ⚠️ Error parsing {ddl_file}: {e}")
        
        print(f"   ✅ Parsed {len(self.table_registry)} table schemas")
    
    def _extract_table_schemas(self, content: str) -> Dict[str, Dict[str, Any]]:
        """Extract table schemas from DDL content"""
        tables = {}
        
        # Find CREATE TABLE statements
        create_pattern = r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*?)\)'
        matches = re.findall(create_pattern, content, re.IGNORECASE | re.DOTALL)
        
        for table_name, columns in matches:
            columns_list = self._parse_columns(columns)
            tables[table_name] = {
                'columns': columns_list,
                'file_path': '',
                'description': f'Table {table_name}'
            }
        
        return tables
    
    def _parse_columns(self, columns_text: str) -> List[Dict[str, str]]:
        """Parse column definitions from DDL"""
        columns = []
        lines = columns_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if line and not line.startswith('--') and not line.startswith('/*'):
                parts = line.split()
                if len(parts) >= 2:
                    col_name = parts[0].strip(',')
                    col_type = parts[1].strip(',')
                    columns.append({
                        'name': col_name,
                        'type': col_type,
                        'description': ''
                    })
        
        return columns
    
    def _find_oozie_workflows(self) -> List[Path]:
        """Find all Oozie workflow files"""
        patterns = [
            "**/coordinators/*.xml",
            "**/workflows/**/*.xml"
        ]
        
        workflow_files = []
        for pattern in patterns:
            files = list(self.hadoop_repo_path.glob(pattern))
            workflow_files.extend(files)
        
        return workflow_files
    
    def _analyze_workflows_complete(self, oozie_workflows: List[Path]) -> List[OozieWorkflow]:
        """Analyze all Oozie workflows with complete details"""
        workflow_analyses = []
        
        for i, workflow_file in enumerate(oozie_workflows):
            try:
                analysis = self._analyze_single_workflow_complete(workflow_file, i + 1)
                if analysis:
                    workflow_analyses.append(analysis)
                    print(f"   ✅ Analyzed: {analysis.name}")
            except Exception as e:
                print(f"   ❌ Error analyzing {workflow_file}: {e}")
        
        return workflow_analyses
    
    def _analyze_single_workflow_complete(self, workflow_file: Path, execution_order: int) -> Optional[OozieWorkflow]:
        """Analyze a single Oozie workflow with complete details"""
        try:
            tree = ET.parse(workflow_file)
            root = tree.getroot()
            
            workflow_name = root.get('name', workflow_file.stem)
            
            # Extract frequency if it's a coordinator
            frequency = "Unknown"
            if 'coordinator' in workflow_file.name.lower():
                frequency = root.get('frequency', 'Unknown')
            
            # Extract actions with deep analysis
            actions = []
            technologies = {}
            input_tables = []
            output_tables = []
            
            for action in root.findall('.//{uri:oozie:workflow:0.5}action'):
                action_info = self._parse_action_complete(action, workflow_file, workflow_name)
                if action_info:
                    actions.append(action_info)
                    
                    # Count technologies
                    tech = action_info.get('technology', 'unknown')
                    technologies[tech] = technologies.get(tech, 0) + 1
                    
                    # Collect input/output tables
                    input_tables.extend(action_info.get('input_tables', []))
                    output_tables.extend(action_info.get('output_tables', []))
            
            # Determine business domain
            business_domain = self._determine_business_domain(workflow_name, actions)
            
            # Determine critical path
            critical_path = self._determine_critical_path(actions)
            
            return OozieWorkflow(
                name=workflow_name,
                file_path=str(workflow_file),
                execution_order=execution_order,
                frequency=frequency,
                total_actions=len(actions),
                actions=actions,
                technologies=technologies,
                input_tables=list(set(input_tables)),
                output_tables=list(set(output_tables)),
                business_domain=business_domain,
                critical_path=critical_path
            )
            
        except Exception as e:
            print(f"   ⚠️ Error parsing {workflow_file}: {e}")
            return None
    
    def _parse_action_complete(self, action_elem, workflow_file: Path, workflow_name: str) -> Optional[Dict[str, Any]]:
        """Parse an individual Oozie action with complete details"""
        action_name = action_elem.get('name')
        if not action_name:
            return None
        
        # Determine action type and script
        action_type = None
        script_path = ""
        technology = ""
        
        # Check for Spark actions
        spark_elem = action_elem.find('.//{uri:oozie:spark-action:0.1}spark')
        if spark_elem is not None:
            action_type = "spark"
            technology = "spark"
            jar_elem = spark_elem.find('{uri:oozie:spark-action:0.1}jar')
            if jar_elem is not None:
                script_path = jar_elem.text or ""
        
        # Check for Pig actions
        pig_elem = action_elem.find('.//{uri:oozie:pig-action:0.1}pig')
        if pig_elem is not None:
            action_type = "pig"
            technology = "pig"
            script_elem = pig_elem.find('{uri:oozie:pig-action:0.1}script')
            if script_elem is not None:
                script_path = script_elem.text or ""
        
        # Check for Hive actions
        hive_elem = action_elem.find('.//{uri:oozie:hive-action:0.2}hive')
        if hive_elem is not None:
            action_type = "hive"
            technology = "hive"
            script_elem = hive_elem.find('{uri:oozie:hive-action:0.2}script')
            if script_elem is not None:
                script_path = script_elem.text or ""
        
        # Check for Shell actions
        shell_elem = action_elem.find('.//{uri:oozie:shell-action:0.3}shell')
        if shell_elem is not None:
            action_type = "shell"
            technology = "shell"
            exec_elem = shell_elem.find('{uri:oozie:shell-action:0.3}exec')
            if exec_elem is not None:
                script_path = exec_elem.text or ""
        
        # Check for Email actions
        email_elem = action_elem.find('.//{uri:oozie:email-action:0.2}email')
        if email_elem is not None:
            action_type = "email"
            technology = "notification"
            script_path = "email_notification"
        
        if not action_type:
            return None
        
        # Extract input/output tables from script
        input_tables, output_tables = self._analyze_script_tables(script_path, workflow_file)
        
        # Determine business purpose
        business_purpose = self._infer_business_purpose(action_name, script_path)
        
        return {
            'action_name': action_name,
            'action_type': action_type,
            'technology': technology,
            'script_path': script_path,
            'script_name': Path(script_path).name if script_path else 'N/A',
            'input_tables': input_tables,
            'output_tables': output_tables,
            'business_purpose': business_purpose,
            'workflow_name': workflow_name
        }
    
    def _analyze_script_tables(self, script_path: str, workflow_file: Path) -> Tuple[List[str], List[str]]:
        """Analyze script to extract input/output tables"""
        input_tables = []
        output_tables = []
        
        if not script_path or script_path == "email_notification":
            return input_tables, output_tables
        
        # Try to find the actual script file
        script_file = self._find_script_file(script_path, workflow_file)
        if not script_file or not script_file.exists():
            return input_tables, output_tables
        
        try:
            content = script_file.read_text()
            
            # Extract table names based on technology
            if script_file.suffix == '.py':  # Spark/PySpark
                input_tables.extend(self._extract_spark_input_tables(content))
                output_tables.extend(self._extract_spark_output_tables(content))
            elif script_file.suffix == '.pig':  # Pig
                input_tables.extend(self._extract_pig_input_tables(content))
                output_tables.extend(self._extract_pig_output_tables(content))
            elif script_file.suffix == '.sql':  # Hive
                input_tables.extend(self._extract_hive_input_tables(content))
                output_tables.extend(self._extract_hive_output_tables(content))
            elif script_file.suffix == '.sh':  # Shell
                input_tables.extend(self._extract_shell_input_tables(content))
                output_tables.extend(self._extract_shell_output_tables(content))
                
        except Exception as e:
            print(f"   ⚠️ Error reading script {script_file}: {e}")
        
        return input_tables, output_tables
    
    def _find_script_file(self, script_path: str, workflow_file: Path) -> Optional[Path]:
        """Find the actual script file"""
        # Clean up the path
        clean_path = script_path.replace('${appPath}', '')
        clean_path = clean_path.replace('${wf:actionData(\'get-datetime\')[\'date\']}', '*')
        
        # Try different search patterns
        patterns = [
            f"**/{clean_path}",
            f"**/{Path(clean_path).name}",
            f"**/*{Path(clean_path).stem}*"
        ]
        
        for pattern in patterns:
            matches = list(self.hadoop_repo_path.glob(pattern))
            if matches:
                return matches[0]
        
        return None
    
    def _extract_spark_input_tables(self, content: str) -> List[str]:
        """Extract input tables from Spark/PySpark code"""
        tables = []
        
        patterns = [
            r'spark\.read\.parquet\(["\']([^"\']+)["\']',
            r'spark\.read\.table\(["\']([^"\']+)["\']',
            r'spark\.read\.format\(["\']([^"\']+)["\']',
            r'\.read\.parquet\(["\']([^"\']+)["\']',
            r'\.read\.table\(["\']([^"\']+)["\']',
            r'\.read\.csv\(["\']([^"\']+)["\']',
            r'\.read\.json\(["\']([^"\']+)["\']'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                table_name = self._extract_table_name_from_path(match)
                if table_name and table_name not in tables:
                    tables.append(table_name)
        
        return tables
    
    def _extract_spark_output_tables(self, content: str) -> List[str]:
        """Extract output tables from Spark/PySpark code"""
        tables = []
        
        patterns = [
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.parquet\(["\']([^"\']+)["\']',
            r'\.write\.parquet\(["\']([^"\']+)["\']',
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.table\(["\']([^"\']+)["\']',
            r'\.write\.table\(["\']([^"\']+)["\']',
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.csv\(["\']([^"\']+)["\']',
            r'\.write\.csv\(["\']([^"\']+)["\']'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    path = match[1] if len(match) > 1 else match[0]
                else:
                    path = match
                table_name = self._extract_table_name_from_path(path)
                if table_name and table_name not in tables:
                    tables.append(table_name)
        
        return tables
    
    def _extract_pig_input_tables(self, content: str) -> List[str]:
        """Extract input tables from Pig code"""
        tables = []
        pattern = r'LOAD\s+[\'"]([^\'"]+)[\'"]'
        matches = re.findall(pattern, content, re.IGNORECASE)
        for match in matches:
            table_name = self._extract_table_name_from_path(match)
            if table_name and table_name not in tables:
                tables.append(table_name)
        return tables
    
    def _extract_pig_output_tables(self, content: str) -> List[str]:
        """Extract output tables from Pig code"""
        tables = []
        pattern = r'STORE\s+\w+\s+INTO\s+[\'"]([^\'"]+)[\'"]'
        matches = re.findall(pattern, content, re.IGNORECASE)
        for match in matches:
            table_name = self._extract_table_name_from_path(match)
            if table_name and table_name not in tables:
                tables.append(table_name)
        return tables
    
    def _extract_hive_input_tables(self, content: str) -> List[str]:
        """Extract input tables from Hive SQL"""
        tables = []
        
        patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'LEFT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'RIGHT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'INNER\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match not in tables:
                    tables.append(match)
        
        return tables
    
    def _extract_hive_output_tables(self, content: str) -> List[str]:
        """Extract output tables from Hive SQL"""
        tables = []
        
        patterns = [
            r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'CREATE\s+EXTERNAL\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match not in tables:
                    tables.append(match)
        
        return tables
    
    def _extract_shell_input_tables(self, content: str) -> List[str]:
        """Extract input tables from Shell scripts"""
        tables = []
        
        patterns = [
            r'/user/[^/]+/[^/]+/[^/]+/([^/\s]+)',
            r'/data/[^/]+/[^/]+/([^/\s]+)',
            r'hdfs://[^/]+/[^/]+/([^/\s]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            tables.extend(matches)
        
        return list(set(tables))
    
    def _extract_shell_output_tables(self, content: str) -> List[str]:
        """Extract output tables from Shell scripts"""
        return self._extract_shell_input_tables(content)
    
    def _extract_table_name_from_path(self, path: str) -> Optional[str]:
        """Extract table name from HDFS path"""
        if not path:
            return None
        
        # Remove common prefixes
        path = path.replace('hdfs://', '').replace('file://', '')
        
        # Extract the last meaningful part
        parts = path.split('/')
        
        # Look for table-like patterns
        for part in reversed(parts):
            if part and part not in ['current', 'data', 'publish', 'staging', 'input', 'output']:
                # Clean up the table name
                table_name = re.sub(r'[^a-zA-Z0-9_]', '', part)
                if table_name:
                    return table_name
        
        return None
    
    def _infer_business_purpose(self, action_name: str, script_path: str) -> str:
        """Infer business purpose from action name and script path"""
        action_lower = action_name.lower()
        script_lower = script_path.lower()
        
        if 'parse' in action_lower or 'parse' in script_lower:
            return "Data Parsing and Ingestion"
        elif 'publish' in action_lower or 'publish' in script_lower:
            return "Data Publishing"
        elif 'notification' in action_lower or 'notification' in script_lower:
            return "Notification and Alerting"
        elif 'check' in action_lower or 'check' in script_lower:
            return "Data Validation and Checking"
        elif 'merge' in action_lower or 'merge' in script_lower:
            return "Data Merging and Consolidation"
        elif 'generate' in action_lower or 'generate' in script_lower:
            return "Data Generation and Transformation"
        elif 'download' in action_lower or 'download' in script_lower:
            return "File Download and Processing"
        elif 'upload' in action_lower or 'upload' in script_lower:
            return "File Upload and Transfer"
        elif 'log' in action_lower or 'log' in script_lower:
            return "Logging and Monitoring"
        else:
            return "Data Processing"
    
    def _determine_business_domain(self, workflow_name: str, actions: List[Dict[str, Any]]) -> str:
        """Determine business domain from workflow name and actions"""
        workflow_lower = workflow_name.lower()
        
        if 'chc' in workflow_lower:
            return "CHC (Clinical Health Center)"
        elif 'gmrn' in workflow_lower or 'globalmrn' in workflow_lower:
            return "GMRN (Global Medical Record Number)"
        elif 'patient' in workflow_lower or 'acct' in workflow_lower:
            return "Patient Account Management"
        elif 'audit' in workflow_lower:
            return "Audit and Compliance"
        elif 'sqoop' in workflow_lower:
            return "Data Ingestion (Sqoop)"
        elif 'swift' in workflow_lower:
            return "Swift Processing"
        elif 'big' in workflow_lower or 'table' in workflow_lower:
            return "Big Data Processing"
        else:
            return "General Data Processing"
    
    def _determine_critical_path(self, actions: List[Dict[str, Any]]) -> List[str]:
        """Determine critical path actions"""
        critical_actions = []
        
        for action in actions:
            action_name = action.get('action_name', '')
            business_purpose = action.get('business_purpose', '')
            
            # Identify critical actions
            if any(keyword in action_name.lower() for keyword in ['parse', 'publish', 'merge', 'generate']):
                critical_actions.append(action_name)
            elif any(keyword in business_purpose.lower() for keyword in ['parsing', 'publishing', 'merging', 'generation']):
                critical_actions.append(action_name)
        
        return critical_actions
    
    def _find_all_scripts(self) -> List[Path]:
        """Find all script files in the repository"""
        script_extensions = ['.py', '.pig', '.sql', '.sh', '.scala']
        script_files = []
        
        for ext in script_extensions:
            files = list(self.hadoop_repo_path.glob(f"**/*{ext}"))
            script_files.extend(files)
        
        return script_files
    
    def _identify_used_scripts(self, all_scripts: List[Path], workflow_analyses: List[OozieWorkflow]):
        """Identify which scripts are used vs unused"""
        used_scripts = set()
        
        # Extract script paths from workflow analyses
        for workflow in workflow_analyses:
            for action in workflow.actions:
                script_path = action.get('script_path', '')
                if script_path:
                    # Try to find the actual script file
                    script_file = self._find_script_file(script_path, Path(workflow.file_path))
                    if script_file:
                        used_scripts.add(script_file)
        
        # Find unused scripts
        self.used_scripts = used_scripts
        self.unused_scripts = set(all_scripts) - used_scripts
        
        print(f"   ✅ Used scripts: {len(self.used_scripts)}")
        print(f"   ✅ Unused scripts: {len(self.unused_scripts)}")
    
    def _deep_analyze_used_scripts(self):
        """Perform deep analysis of used scripts"""
        for script_file in self.used_scripts:
            try:
                # Find which workflow actions use this script
                workflow_actions = []
                for workflow in self.workflows:
                    for i, action in enumerate(workflow.actions):
                        if script_file.name in action.get('script_path', ''):
                            workflow_actions.append({
                                'workflow_name': workflow.name,
                                'action_name': action.get('action_name', ''),
                                'execution_order': i + 1
                            })
                
                # Perform deep analysis
                analysis = self._analyze_script_deeply(script_file, workflow_actions)
                if analysis:
                    self.workflow_actions.append(analysis)
                    
            except Exception as e:
                print(f"   ⚠️ Error analyzing {script_file}: {e}")
    
    def _analyze_script_deeply(self, script_path: Path, workflow_actions: List[Dict[str, Any]]) -> Optional[WorkflowAction]:
        """Perform deep analysis of a single script"""
        try:
            content = script_path.read_text()
            technology = self._determine_technology(script_path)
            
            # Get workflow action info
            workflow_info = workflow_actions[0] if workflow_actions else {}
            
            # Extract transformations, joins, etc.
            transformations = self._extract_transformations(content, technology)
            joins = self._extract_joins(content, technology)
            data_quality_rules = self._extract_data_quality_rules(content, technology)
            business_rules = self._extract_business_rules(content)
            column_mappings = self._extract_column_mappings(content, technology)
            
            # Extract input/output tables
            input_tables, output_tables = self._extract_table_mappings(script_path, content, technology)
            
            # Calculate complexity
            complexity_score = self._calculate_complexity(content)
            
            # Determine business purpose
            business_purpose = self._determine_business_purpose_from_content(content, script_path.name)
            
            return WorkflowAction(
                workflow_name=workflow_info.get('workflow_name', 'Unknown'),
                action_name=workflow_info.get('action_name', script_path.stem),
                action_type=technology,
                technology=technology,
                script_path=str(script_path),
                script_name=script_path.name,
                execution_order=workflow_info.get('execution_order', 0),
                input_tables=input_tables,
                output_tables=output_tables,
                business_purpose=business_purpose,
                transformations=transformations,
                joins=joins,
                data_quality_rules=data_quality_rules,
                business_rules=business_rules,
                column_mappings=column_mappings,
                complexity_score=complexity_score,
                dependencies=[]
            )
            
        except Exception as e:
            print(f"   ⚠️ Error analyzing {script_path}: {e}")
            return None
    
    def _determine_technology(self, script_path: Path) -> str:
        """Determine the technology of a script"""
        ext = script_path.suffix.lower()
        
        if ext == '.py':
            return 'spark'
        elif ext == '.pig':
            return 'pig'
        elif ext == '.sql':
            return 'hive'
        elif ext == '.sh':
            return 'shell'
        elif ext == '.scala':
            return 'spark'
        else:
            return 'unknown'
    
    def _extract_transformations(self, content: str, technology: str) -> List[Dict[str, Any]]:
        """Extract transformations based on technology"""
        transformations = []
        
        if technology == 'spark':
            patterns = [
                (r'\.select\(([^)]+)\)', 'select'),
                (r'\.filter\(([^)]+)\)', 'filter'),
                (r'\.groupBy\(([^)]+)\)', 'groupBy'),
                (r'\.agg\(([^)]+)\)', 'aggregate'),
                (r'\.withColumn\(["\']([^"\']+)["\'],\s*([^)]+)\)', 'withColumn'),
                (r'\.drop\(["\']([^"\']+)["\']\)', 'drop'),
                (r'\.distinct\(\)', 'distinct'),
                (r'\.repartition\(([^)]+)\)', 'repartition')
            ]
        elif technology == 'pig':
            patterns = [
                (r'FOREACH\s+([^;]+)', 'foreach'),
                (r'FILTER\s+([^;]+)', 'filter'),
                (r'GROUP\s+([^;]+)', 'group'),
                (r'ORDER\s+([^;]+)', 'order'),
                (r'DISTINCT\s+([^;]+)', 'distinct')
            ]
        elif technology == 'hive':
            patterns = [
                (r'SELECT\s+([^FROM]+)', 'select'),
                (r'WHERE\s+([^GROUP|ORDER|HAVING]+)', 'filter'),
                (r'GROUP\s+BY\s+([^HAVING|ORDER]+)', 'groupBy'),
                (r'ORDER\s+BY\s+([^LIMIT]+)', 'orderBy'),
                (r'HAVING\s+([^ORDER|LIMIT]+)', 'having')
            ]
        else:
            return transformations
        
        for pattern, transform_type in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                transformations.append({
                    'type': transform_type,
                    'expression': match if isinstance(match, str) else str(match),
                    'description': f'{technology.upper()} {transform_type}: {match}'
                })
        
        return transformations
    
    def _extract_joins(self, content: str, technology: str) -> List[Dict[str, Any]]:
        """Extract join operations based on technology"""
        joins = []
        
        if technology == 'spark':
            patterns = [
                r'\.join\(([^,]+),\s*([^,]+),\s*["\']([^"\']+)["\']\)',
                r'\.join\(([^,]+),\s*([^)]+)\)',
                r'\.leftJoin\(([^,]+),\s*([^)]+)\)',
                r'\.rightJoin\(([^,]+),\s*([^)]+)\)'
            ]
        elif technology == 'hive':
            patterns = [
                r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+ON\s+([^WHERE|GROUP|ORDER]+)',
                r'LEFT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+ON\s+([^WHERE|GROUP|ORDER]+)',
                r'RIGHT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+ON\s+([^WHERE|GROUP|ORDER]+)'
            ]
        else:
            return joins
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                joins.append({
                    'left_table': match[0].strip(),
                    'right_table': match[1].strip(),
                    'join_type': 'inner',
                    'join_condition': match[1] if len(match) >= 2 else '',
                    'description': f"Join {match[0]} with {match[1]}"
                })
        
        return joins
    
    def _extract_data_quality_rules(self, content: str, technology: str) -> List[Dict[str, Any]]:
        """Extract data quality rules based on technology"""
        rules = []
        
        if technology == 'spark':
            patterns = [
                (r'\.isNull\(\)', 'null_check'),
                (r'\.isNotNull\(\)', 'not_null_check'),
                (r'\.isNaN\(\)', 'nan_check'),
                (r'\.isIn\(([^)]+)\)', 'value_in_list'),
                (r'\.between\(([^,]+),\s*([^)]+)\)', 'range_check'),
                (r'\.contains\(["\']([^"\']+)["\']\)', 'contains_check')
            ]
        else:
            return rules
        
        for pattern, rule_type in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                rules.append({
                    'type': rule_type,
                    'condition': match if isinstance(match, str) else str(match),
                    'description': f"Data quality rule: {rule_type}"
                })
        
        return rules
    
    def _extract_business_rules(self, content: str) -> List[str]:
        """Extract business rules from code comments"""
        rules = []
        
        comment_patterns = [
            r'#\s*Business\s+Rule[:\s]+([^\n]+)',
            r'#\s*Rule[:\s]+([^\n]+)',
            r'#\s*Logic[:\s]+([^\n]+)',
            r'/\*\s*Business\s+Rule[:\s]+([^*]+)\*/',
            r'//\s*Business\s+Rule[:\s]+([^\n]+)'
        ]
        
        for pattern in comment_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                rules.append(match.strip())
        
        return rules
    
    def _extract_column_mappings(self, content: str, technology: str) -> List[Dict[str, Any]]:
        """Extract column mappings based on technology"""
        mappings = []
        
        if technology == 'spark':
            patterns = [
                (r'\.withColumn\(["\']([^"\']+)["\'],\s*([^)]+)\)', 'column_transform'),
                (r'\.select\(([^)]+)\)', 'column_select'),
                (r'\.alias\(["\']([^"\']+)["\']\)', 'column_alias')
            ]
        else:
            return mappings
        
        for pattern, mapping_type in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                mappings.append({
                    'type': mapping_type,
                    'source_column': match[0] if isinstance(match, tuple) else '',
                    'target_column': match[0] if isinstance(match, str) else match[0],
                    'transformation': match[1] if isinstance(match, tuple) and len(match) > 1 else '',
                    'description': f"{mapping_type}: {match}"
                })
        
        return mappings
    
    def _extract_table_mappings(self, script_path: Path, content: str, technology: str) -> Tuple[List[str], List[str]]:
        """Extract input/output table mappings"""
        input_tables = []
        output_tables = []
        
        if technology == 'spark':
            input_tables = self._extract_spark_input_tables(content)
            output_tables = self._extract_spark_output_tables(content)
        elif technology == 'pig':
            input_tables = self._extract_pig_input_tables(content)
            output_tables = self._extract_pig_output_tables(content)
        elif technology == 'hive':
            input_tables = self._extract_hive_input_tables(content)
            output_tables = self._extract_hive_output_tables(content)
        
        return input_tables, output_tables
    
    def _calculate_complexity(self, content: str) -> int:
        """Calculate complexity score of the script"""
        score = 0
        
        # Count different complexity indicators
        score += len(re.findall(r'\.join\(', content, re.IGNORECASE)) * 3
        score += len(re.findall(r'\.groupBy\(', content, re.IGNORECASE)) * 2
        score += len(re.findall(r'\.withColumn\(', content, re.IGNORECASE)) * 1
        score += len(re.findall(r'\.filter\(', content, re.IGNORECASE)) * 1
        score += len(re.findall(r'\.select\(', content, re.IGNORECASE)) * 1
        score += len(re.findall(r'if\s+', content, re.IGNORECASE)) * 2
        score += len(re.findall(r'for\s+', content, re.IGNORECASE)) * 2
        score += len(re.findall(r'while\s+', content, re.IGNORECASE)) * 2
        
        return min(score, 100)  # Cap at 100
    
    def _determine_business_purpose_from_content(self, content: str, script_name: str) -> str:
        """Determine business purpose from script content"""
        content_lower = content.lower()
        script_lower = script_name.lower()
        
        purpose_keywords = {
            'validation': ['validate', 'check', 'verify', 'quality'],
            'transformation': ['transform', 'convert', 'map', 'process'],
            'aggregation': ['aggregate', 'sum', 'count', 'group'],
            'filtering': ['filter', 'where', 'condition'],
            'joining': ['join', 'merge', 'combine'],
            'cleansing': ['clean', 'scrub', 'normalize'],
            'enrichment': ['enrich', 'enhance', 'add'],
            'deduplication': ['dedup', 'distinct', 'unique']
        }
        
        purposes = []
        for purpose, keywords in purpose_keywords.items():
            if any(keyword in content_lower or keyword in script_lower for keyword in keywords):
                purposes.append(purpose)
        
        if purposes:
            return ', '.join(purposes)
        else:
            return 'Data processing'
    
    def _create_complete_pipeline_flow(self) -> List[CompletePipelineFlow]:
        """Create complete pipeline flow with all details"""
        pipeline_flow = []
        
        for workflow in self.workflows:
            for i, action in enumerate(workflow.actions):
                # Find corresponding workflow action analysis
                workflow_action = None
                for wa in self.workflow_actions:
                    if (wa.workflow_name == workflow.name and 
                        wa.action_name == action.get('action_name', '')):
                        workflow_action = wa
                        break
                
                if workflow_action:
                    flow_item = CompletePipelineFlow(
                        workflow_name=workflow.name,
                        workflow_frequency=workflow.frequency,
                        step_number=i + 1,
                        action_name=action.get('action_name', ''),
                        script_name=action.get('script_name', ''),
                        technology=action.get('technology', ''),
                        execution_order=workflow_action.execution_order,
                        input_tables=', '.join(workflow_action.input_tables),
                        output_tables=', '.join(workflow_action.output_tables),
                        business_purpose=workflow_action.business_purpose,
                        transformations_count=len(workflow_action.transformations),
                        joins_count=len(workflow_action.joins),
                        data_quality_rules_count=len(workflow_action.data_quality_rules),
                        complexity_score=workflow_action.complexity_score,
                        is_critical_path=action.get('action_name', '') in workflow.critical_path
                    )
                else:
                    flow_item = CompletePipelineFlow(
                        workflow_name=workflow.name,
                        workflow_frequency=workflow.frequency,
                        step_number=i + 1,
                        action_name=action.get('action_name', ''),
                        script_name=action.get('script_name', ''),
                        technology=action.get('technology', ''),
                        execution_order=i + 1,
                        input_tables=', '.join(action.get('input_tables', [])),
                        output_tables=', '.join(action.get('output_tables', [])),
                        business_purpose=action.get('business_purpose', ''),
                        transformations_count=0,
                        joins_count=0,
                        data_quality_rules_count=0,
                        complexity_score=0,
                        is_critical_path=action.get('action_name', '') in workflow.critical_path
                    )
                
                pipeline_flow.append(flow_item)
        
        return pipeline_flow
    
    def _generate_complete_report(self, workflow_analyses: List[OozieWorkflow], 
                                 complete_pipeline: List[CompletePipelineFlow]) -> Dict[str, Any]:
        """Generate complete analysis report"""
        
        # Calculate statistics
        total_workflows = len(workflow_analyses)
        total_scripts = len(self.used_scripts) + len(self.unused_scripts)
        used_scripts_count = len(self.used_scripts)
        unused_scripts_count = len(self.unused_scripts)
        
        # Technology breakdown
        tech_breakdown = {}
        for workflow_action in self.workflow_actions:
            tech = workflow_action.technology
            tech_breakdown[tech] = tech_breakdown.get(tech, 0) + 1
        
        # Workflow frequency analysis
        frequency_analysis = {}
        for workflow in workflow_analyses:
            freq = workflow.frequency
            frequency_analysis[freq] = frequency_analysis.get(freq, 0) + 1
        
        # Business domain analysis
        domain_analysis = {}
        for workflow in workflow_analyses:
            domain = workflow.business_domain
            domain_analysis[domain] = domain_analysis.get(domain, 0) + 1
        
        return {
            'summary': {
                'total_workflows': total_workflows,
                'total_scripts': total_scripts,
                'used_scripts_count': used_scripts_count,
                'unused_scripts_count': unused_scripts_count,
                'usage_percentage': (used_scripts_count / total_scripts * 100) if total_scripts > 0 else 0,
                'technology_breakdown': tech_breakdown,
                'frequency_analysis': frequency_analysis,
                'domain_analysis': domain_analysis,
                'table_registry_size': len(self.table_registry)
            },
            'workflows': [asdict(workflow) for workflow in workflow_analyses],
            'workflow_actions': [asdict(wa) for wa in self.workflow_actions],
            'complete_pipeline': [asdict(cp) for cp in complete_pipeline],
            'unused_scripts': [str(script) for script in self.unused_scripts],
            'table_registry': self.table_registry
        }
    
    def generate_complete_excel_report(self, analysis: Dict[str, Any], 
                                     output_file: str = "complete_hadoop_oozie_flow_analysis.xlsx"):
        """Generate comprehensive Excel report with all details"""
        print(f"📊 Generating complete Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Summary Sheet
            summary_data = [analysis['summary']]
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Workflows Sheet
            workflows_data = []
            for workflow in analysis['workflows']:
                workflows_data.append({
                    'Execution Order': workflow['execution_order'],
                    'Workflow Name': workflow['name'],
                    'File Path': workflow['file_path'],
                    'Frequency': workflow['frequency'],
                    'Total Actions': workflow['total_actions'],
                    'Business Domain': workflow['business_domain'],
                    'Technologies': str(workflow['technologies']),
                    'Input Tables': ', '.join(workflow['input_tables']),
                    'Output Tables': ', '.join(workflow['output_tables']),
                    'Critical Path': ', '.join(workflow['critical_path'])
                })
            
            pd.DataFrame(workflows_data).to_excel(writer, sheet_name='Workflows', index=False)
            
            # Complete Pipeline Flow Sheet
            pipeline_data = []
            for flow in analysis['complete_pipeline']:
                pipeline_data.append({
                    'Workflow Name': flow['workflow_name'],
                    'Workflow Frequency': flow['workflow_frequency'],
                    'Step Number': flow['step_number'],
                    'Action Name': flow['action_name'],
                    'Script Name': flow['script_name'],
                    'Technology': flow['technology'],
                    'Execution Order': flow['execution_order'],
                    'Input Tables': flow['input_tables'],
                    'Output Tables': flow['output_tables'],
                    'Business Purpose': flow['business_purpose'],
                    'Transformations Count': flow['transformations_count'],
                    'Joins Count': flow['joins_count'],
                    'Data Quality Rules Count': flow['data_quality_rules_count'],
                    'Complexity Score': flow['complexity_score'],
                    'Is Critical Path': flow['is_critical_path']
                })
            
            pd.DataFrame(pipeline_data).to_excel(writer, sheet_name='Complete Pipeline Flow', index=False)
            
            # Workflow Actions Detail Sheet
            actions_data = []
            for action in analysis['workflow_actions']:
                actions_data.append({
                    'Workflow Name': action['workflow_name'],
                    'Action Name': action['action_name'],
                    'Technology': action['technology'],
                    'Script Name': action['script_name'],
                    'Script Path': action['script_path'],
                    'Execution Order': action['execution_order'],
                    'Input Tables': ', '.join(action['input_tables']),
                    'Output Tables': ', '.join(action['output_tables']),
                    'Business Purpose': action['business_purpose'],
                    'Transformations Count': len(action['transformations']),
                    'Joins Count': len(action['joins']),
                    'Data Quality Rules Count': len(action['data_quality_rules']),
                    'Business Rules Count': len(action['business_rules']),
                    'Column Mappings Count': len(action['column_mappings']),
                    'Complexity Score': action['complexity_score']
                })
            
            pd.DataFrame(actions_data).to_excel(writer, sheet_name='Workflow Actions Detail', index=False)
            
            # Transformations Sheet
            transformations_data = []
            for action in analysis['workflow_actions']:
                for transform in action['transformations']:
                    transformations_data.append({
                        'Workflow': action['workflow_name'],
                        'Action': action['action_name'],
                        'Script': action['script_name'],
                        'Technology': action['technology'],
                        'Transformation Type': transform['type'],
                        'Expression': transform['expression'],
                        'Description': transform['description']
                    })
            
            pd.DataFrame(transformations_data).to_excel(writer, sheet_name='Transformations', index=False)
            
            # Joins Sheet
            joins_data = []
            for action in analysis['workflow_actions']:
                for join in action['joins']:
                    joins_data.append({
                        'Workflow': action['workflow_name'],
                        'Action': action['action_name'],
                        'Script': action['script_name'],
                        'Technology': action['technology'],
                        'Left Table': join['left_table'],
                        'Right Table': join['right_table'],
                        'Join Type': join['join_type'],
                        'Join Condition': join['join_condition'],
                        'Description': join['description']
                    })
            
            pd.DataFrame(joins_data).to_excel(writer, sheet_name='Joins', index=False)
            
            # Data Quality Rules Sheet
            dq_data = []
            for action in analysis['workflow_actions']:
                for rule in action['data_quality_rules']:
                    dq_data.append({
                        'Workflow': action['workflow_name'],
                        'Action': action['action_name'],
                        'Script': action['script_name'],
                        'Technology': action['technology'],
                        'Rule Type': rule['type'],
                        'Condition': rule['condition'],
                        'Description': rule['description']
                    })
            
            pd.DataFrame(dq_data).to_excel(writer, sheet_name='Data Quality Rules', index=False)
            
            # Business Rules Sheet
            business_rules_data = []
            for action in analysis['workflow_actions']:
                for rule in action['business_rules']:
                    business_rules_data.append({
                        'Workflow': action['workflow_name'],
                        'Action': action['action_name'],
                        'Script': action['script_name'],
                        'Technology': action['technology'],
                        'Business Rule': rule
                    })
            
            pd.DataFrame(business_rules_data).to_excel(writer, sheet_name='Business Rules', index=False)
            
            # Column Mappings Sheet
            column_mappings_data = []
            for action in analysis['workflow_actions']:
                for mapping in action['column_mappings']:
                    column_mappings_data.append({
                        'Workflow': action['workflow_name'],
                        'Action': action['action_name'],
                        'Script': action['script_name'],
                        'Technology': action['technology'],
                        'Mapping Type': mapping['type'],
                        'Source Column': mapping['source_column'],
                        'Target Column': mapping['target_column'],
                        'Transformation': mapping['transformation'],
                        'Description': mapping['description']
                    })
            
            pd.DataFrame(column_mappings_data).to_excel(writer, sheet_name='Column Mappings', index=False)
            
            # Unused Scripts Sheet
            unused_data = []
            for script in analysis['unused_scripts']:
                script_path = Path(script)
                unused_data.append({
                    'Unused Script': str(script_path),
                    'Technology': script_path.suffix,
                    'Size': f"{script_path.stat().st_size} bytes" if script_path.exists() else "Unknown"
                })
            
            pd.DataFrame(unused_data).to_excel(writer, sheet_name='Unused Scripts', index=False)
            
            # Table Registry Sheet
            table_registry_data = []
            for table_name, table_info in analysis['table_registry'].items():
                columns_str = ', '.join([col['name'] for col in table_info['columns']])
                table_registry_data.append({
                    'Table Name': table_name,
                    'Columns': columns_str,
                    'Column Count': len(table_info['columns']),
                    'Description': table_info['description']
                })
            
            pd.DataFrame(table_registry_data).to_excel(writer, sheet_name='Table Registry', index=False)
        
        print(f"✅ Complete Excel report generated: {output_file}")

def main():
    """Main function"""
    print("🚀 Complete Hadoop Oozie Flow + Deep Code Analysis")
    print("=" * 70)
    
    # Use current repository
    repo_path = "./OneDrive_1_7-25-2025/Hadoop/app-data-ingestion"
    
    # Initialize analyzer
    analyzer = CompleteHadoopAnalyzer(repo_path)
    
    # Perform complete analysis
    analysis = analyzer.analyze_complete_hadoop_repository()
    
    # Generate Excel report
    analyzer.generate_complete_excel_report(analysis)
    
    # Print summary
    summary = analysis['summary']
    print(f"\n📊 Complete Analysis Results!")
    print(f"   Total Workflows: {summary['total_workflows']}")
    print(f"   Total Scripts: {summary['total_scripts']}")
    print(f"   Used Scripts: {summary['used_scripts_count']}")
    print(f"   Unused Scripts: {summary['unused_scripts_count']}")
    print(f"   Usage: {summary['usage_percentage']:.1f}%")
    
    print(f"\n🔧 Technology Breakdown:")
    for tech, count in summary['technology_breakdown'].items():
        print(f"   {tech.upper()}: {count} scripts")
    
    print(f"\n🏢 Business Domains:")
    for domain, count in summary['domain_analysis'].items():
        print(f"   {domain}: {count} workflows")
    
    print(f"\n✅ Complete report generated: complete_hadoop_oozie_flow_analysis.xlsx")

if __name__ == "__main__":
    main()
