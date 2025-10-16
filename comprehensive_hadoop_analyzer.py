#!/usr/bin/env python3
"""
Comprehensive Hadoop Repository Analysis Tool
============================================
Enhanced version with multi-repo support and script dependency analysis.

Usage:
    python comprehensive_hadoop_analyzer.py <hadoop_repo_path>
    python comprehensive_hadoop_analyzer.py <repo1> <repo2> <repo3> ...
    python comprehensive_hadoop_analyzer.py --file <path_to_repo_list.txt>

Features:
- Multi-repository analysis with separate Excel files
- Script dependency analysis (direct and indirect usage)
- Deep code analysis for Python, Pig, SQL, Shell scripts
- Comprehensive Excel reports with detailed sheets
- Execution order detection based on coordinators
- Business domain classification
- Technology breakdown
"""

import sys
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd
import re
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import glob

class ComprehensiveHadoopAnalyzer:
    """Comprehensive Hadoop repository analyzer with multi-repo support"""
    
    def __init__(self, hadoop_path: str):
        self.hadoop_path = Path(hadoop_path)
        self.ddl_tables = {}
        self.workflow_analyses = []
        self.all_scripts = []
        self.used_scripts = set()
        self.unused_scripts = []
        self.script_dependencies = {}
        
    def analyze(self) -> str:
        """Main analysis method"""
        print("🚀 Comprehensive Hadoop Repository Analysis")
        print("=" * 60)
        
        if not self.hadoop_path.exists():
            raise ValueError(f"Hadoop path does not exist: {self.hadoop_path}")
        
        print(f"📁 Analyzing: {self.hadoop_path}")
        
        # Step 1: Parse DDL files
        print("\n🔍 Step 1: Parsing DDL files...")
        self._parse_ddl_files()
        
        # Step 2: Analyze Oozie workflows
        print("\n🔍 Step 2: Analyzing Oozie workflows...")
        oozie_workflows = self._find_oozie_workflows()
        self._analyze_workflows_comprehensive(oozie_workflows)
        
        # Step 3: Find all scripts
        print("\n🔍 Step 3: Finding all scripts...")
        self._find_all_scripts()
        
        # Step 4: Identify used vs unused scripts with dependencies
        print("\n🔍 Step 4: Identifying used vs unused scripts...")
        self._identify_unused_scripts_comprehensive()
        
        # Step 5: Analyze business domains
        print("\n🔍 Step 5: Analyzing business domains...")
        self._analyze_business_domains(self.workflow_analyses)
        
        # Step 6: Create pipeline flows
        print("\n🔍 Step 6: Creating pipeline flows...")
        self._create_pipeline_flows()
        
        # Step 7: Create execution steps
        print("\n🔍 Step 7: Creating execution steps...")
        self._create_execution_steps()
        
        # Step 8: Analyze technology flows
        print("\n🔍 Step 8: Analyzing technology flows...")
        self._analyze_technology_flows()
        
        # Step 9: Create detailed data flows
        print("\n🔍 Step 9: Creating detailed data flows...")
        self._create_detailed_data_flows()
        
        # Step 10: Generate comprehensive Excel report
        print("\n🔍 Step 10: Generating comprehensive Excel report...")
        output_file = self._generate_comprehensive_excel_report()
        
        # Print summary
        self._print_comprehensive_summary()
        
        return output_file
    
    def _parse_ddl_files(self):
        """Parse DDL files to extract table schemas"""
        ddl_files = list(self.hadoop_path.rglob("*.sql"))
        
        for ddl_file in ddl_files:
            try:
                with open(ddl_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Extract table names and columns
                table_matches = re.findall(r'CREATE\s+TABLE\s+(\w+)', content, re.IGNORECASE)
                for table in table_matches:
                    self.ddl_tables[table.lower()] = {
                        'file': str(ddl_file),
                        'schema': content
                    }
                    
            except Exception as e:
                print(f"   ⚠️ Error parsing DDL {ddl_file}: {e}")
        
        print(f"   ✅ Parsed {len(self.ddl_tables)} table schemas")
    
    def _find_oozie_workflows(self) -> List[Path]:
        """Find all Oozie workflow and coordinator files"""
        patterns = [
            "**/oozie/**/*.xml",
            "**/*workflow*.xml", 
            "**/*coordinator*.xml",
            "**/coordinators/*.xml"
        ]
        
        workflows = []
        for pattern in patterns:
            workflows.extend(self.hadoop_path.rglob(pattern))
        
        # Remove duplicates and validate XML
        unique_workflows = list(set(workflows))
        validated_workflows = []
        
        for workflow_file in unique_workflows:
            try:
                tree = ET.parse(workflow_file)
                root = tree.getroot()
                
                # Check if it's an Oozie XML file
                if any(tag in root.tag.lower() for tag in ['workflow', 'coordinator']):
                    validated_workflows.append(workflow_file)
                    
            except Exception as e:
                print(f"   ⚠️ Error parsing {workflow_file}: {e}")
        
        print(f"   📋 Found {len(validated_workflows)} Oozie workflow files")
        return validated_workflows
    
    def _determine_workflow_execution_order(self, oozie_workflows: List[Path]) -> Dict[str, int]:
        """Determine execution order based on coordinators and naming patterns"""
        workflow_order = {}
        
        # Separate coordinators and workflows
        coordinators = []
        workflows = []
        
        for workflow_file in oozie_workflows:
            try:
                tree = ET.parse(workflow_file)
                root = tree.getroot()
                
                if 'coordinator' in root.tag.lower():
                    coordinators.append((workflow_file, root))
                else:
                    workflows.append((workflow_file, root))
                    
            except Exception as e:
                print(f"   ⚠️ Error parsing {workflow_file}: {e}")
        
        print(f"   📊 Found {len(coordinators)} coordinators and {len(workflows)} workflows")
        
        # Analyze coordinators and their scheduled workflows
        coordinator_workflows = {}
        for coord_file, coord_root in coordinators:
            try:
                coord_name = coord_root.get('name', coord_file.stem)
                frequency = coord_root.get('frequency', 'Unknown')
                
                # Find the workflow this coordinator schedules
                workflow_elem = coord_root.find('.//{uri:oozie:coordinator:0.2}workflow')
                if workflow_elem is not None:
                    app_path_elem = workflow_elem.find('.//{uri:oozie:coordinator:0.2}app-path')
                    if app_path_elem is not None:
                        workflow_path = app_path_elem.text
                        workflow_name = Path(workflow_path).stem if workflow_path else coord_file.stem
                        
                        coordinator_workflows[workflow_name] = {
                            'coordinator': coord_name,
                            'frequency': frequency,
                            'order': 1
                        }
                        
                        print(f"   📅 Coordinator '{coord_name}' schedules workflow '{workflow_name}' (freq: {frequency})")
                        
            except Exception as e:
                print(f"   ⚠️ Error analyzing coordinator {coord_file}: {e}")
        
        # Assign order to coordinated workflows first
        order_counter = 1
        for workflow_name, coord_info in coordinator_workflows.items():
            workflow_order[workflow_name] = order_counter
            order_counter += 1
        
        # Sort remaining workflows by naming patterns
        remaining_workflows = []
        for workflow_file, workflow_root in workflows:
            workflow_name = workflow_root.get('name', workflow_file.stem)
            if workflow_name not in workflow_order:
                remaining_workflows.append((workflow_file, workflow_root, workflow_name))
        
        def get_execution_priority(workflow_name):
            """Determine execution priority based on naming patterns"""
            name_lower = workflow_name.lower()
            
            if any(pattern in name_lower for pattern in ['prebdf', 'pre', 'ingest', 'sqoop']):
                return 1
            elif any(pattern in name_lower for pattern in ['postbdf', 'post', 'transform', 'process']):
                return 2
            elif any(pattern in name_lower for pattern in ['audit', 'reconcile', 'validate']):
                return 3
            elif any(pattern in name_lower for pattern in ['purge', 'cleanup', 'archive']):
                return 4
            else:
                return 5
        
        remaining_workflows.sort(key=lambda x: get_execution_priority(x[2]))
        
        # Assign order to remaining workflows
        for workflow_file, workflow_root, workflow_name in remaining_workflows:
            workflow_order[workflow_name] = order_counter
            order_counter += 1
        
        print(f"   📋 Workflow execution order determined: {len(workflow_order)} workflows")
        return workflow_order
    
    def _analyze_workflows_comprehensive(self, oozie_workflows: List[Path]):
        """Analyze all Oozie workflows comprehensively"""
        workflow_order = self._determine_workflow_execution_order(oozie_workflows)
        
        for i, workflow_file in enumerate(oozie_workflows):
            try:
                tree = ET.parse(workflow_file)
                root = tree.getroot()
                workflow_name = root.get('name', workflow_file.stem)
                execution_order = workflow_order.get(workflow_name, i + 1)
                
                analysis = self._analyze_single_workflow_comprehensive(workflow_file, execution_order)
                if analysis:
                    analysis['workflow_execution_order'] = execution_order
                    self.workflow_analyses.append(analysis)
                    print(f"   ✅ Analyzed: {analysis['name']} (Order: {execution_order})")
                    
            except Exception as e:
                print(f"   ❌ Error analyzing {workflow_file}: {e}")
    
    def _analyze_single_workflow_comprehensive(self, workflow_file: Path, execution_order: int) -> Optional[Dict[str, Any]]:
        """Analyze a single Oozie workflow comprehensively"""
        try:
            tree = ET.parse(workflow_file)
            root = tree.getroot()
            
            workflow_name = root.get('name', workflow_file.stem)
            
            # Extract frequency if it's a coordinator
            frequency = "Unknown"
            if 'coordinator' in workflow_file.name.lower():
                frequency = root.get('frequency', 'Unknown')
            
            # Extract actions with comprehensive analysis
            actions = []
            technologies = {}
            input_tables = []
            output_tables = []
            data_sources = []
            data_targets = []
            
            for action in root.findall('.//{uri:oozie:workflow:0.5}action'):
                action_info = self._parse_action_comprehensive(action, workflow_file, workflow_name)
                if action_info:
                    actions.append(action_info)
                    
                    # Count technologies
                    tech = action_info.get('technology', 'unknown')
                    technologies[tech] = technologies.get(tech, 0) + 1
                    
                    # Collect tables and data sources
                    input_tables.extend(action_info.get('input_tables', []))
                    output_tables.extend(action_info.get('output_tables', []))
                    data_sources.extend(action_info.get('data_sources', []))
                    data_targets.extend(action_info.get('data_targets', []))
            
            # Determine business domain
            business_domain = self._determine_business_domain(workflow_name, actions)
            
            # Determine execution order and frequency
            frequency = self._determine_frequency(workflow_name, {})
            
            return {
                'name': workflow_name,
                'file': str(workflow_file),
                'execution_order': execution_order,
                'frequency': frequency,
                'total_actions': len(actions),
                'actions': actions,
                'technologies': technologies,
                'input_tables': list(set(input_tables)),
                'output_tables': list(set(output_tables)),
                'data_sources': list(set(data_sources)),
                'data_targets': list(set(data_targets)),
                'business_domain': business_domain,
                'complexity_score': self._calculate_workflow_complexity(actions),
                'dependencies': self._extract_workflow_dependencies(workflow_name, actions)
            }
            
        except Exception as e:
            print(f"   ⚠️ Error analyzing workflow {workflow_file}: {e}")
            return None
    
    def _parse_action_comprehensive(self, action_elem, workflow_file: Path, workflow_name: str) -> Optional[Dict[str, Any]]:
        """Parse an Oozie action element comprehensively"""
        try:
            action_name = action_elem.get('name', 'Unknown')
            
            # Determine technology and extract script path
            technology = 'Unknown'
            script_path = None
            
            # Check for different action types with comprehensive parsing
            spark_elem = action_elem.find('.//{uri:oozie:spark-action:0.1}spark')
            if spark_elem is not None:
                technology = 'Spark'
                # Look for jar element in multiple ways
                jar_elem = None
                for child in spark_elem:
                    if child.tag.endswith('jar'):
                        jar_elem = child
                        break
                if jar_elem is not None:
                    script_path = jar_elem.text
            
            pig_elem = action_elem.find('.//pig')
            if pig_elem is not None:
                technology = 'Pig'
                script_elem = pig_elem.find('.//script')
                if script_elem is not None:
                    script_path = script_elem.text
            
            hive_elem = action_elem.find('.//{uri:oozie:hive-action:0.1}hive')
            if hive_elem is not None:
                technology = 'Hive'
                script_elem = hive_elem.find('.//{uri:oozie:hive-action:0.1}script')
                if script_elem is not None:
                    script_path = script_elem.text
            
            shell_elem = action_elem.find('.//{uri:oozie:shell-action:0.1}shell')
            if shell_elem is not None:
                technology = 'Shell'
                # Look for exec element in multiple ways
                exec_elem = None
                for child in shell_elem:
                    if child.tag.endswith('exec'):
                        exec_elem = child
                        break
                if exec_elem is not None:
                    script_path = exec_elem.text
            
            # Extract script name
            script_name = "N/A"
            if script_path:
                script_name = self._extract_script_name(script_path)
                self.used_scripts.add(script_name)
            
            # Analyze script for business logic
            business_purpose = self._determine_script_business_purpose(action_name, script_path)
            
            # Perform deep analysis if script file exists
            deep_analysis = {'input_tables': [], 'output_tables': [], 'data_sources': [], 'data_targets': []}
            if script_path:
                script_file = self._find_script_file(script_name)
                if script_file and script_file.exists():
                    deep_analysis = self._analyze_script_comprehensive(script_file)
            
            return {
                'name': action_name,
                'technology': technology,
                'script_path': script_path,
                'script_name': script_name,
                'business_purpose': business_purpose,
                'input_tables': deep_analysis.get('input_tables', []),
                'output_tables': deep_analysis.get('output_tables', []),
                'data_sources': deep_analysis.get('data_sources', []),
                'data_targets': deep_analysis.get('data_targets', []),
                'transformations': deep_analysis.get('transformations', []),
                'business_logic': deep_analysis.get('business_logic', [])
            }
            
        except Exception as e:
            print(f"   ⚠️ Error parsing action: {e}")
            return None
    
    def _extract_script_name(self, script_path: str) -> str:
        """Extract script name from path, handling Oozie variables"""
        if not script_path:
            return "N/A"
        
        # Remove Oozie variables and extract filename
        clean_path = re.sub(r'\$\{[^}]+\}', '', script_path)
        clean_path = clean_path.replace('${', '').replace('}', '')
        
        return Path(clean_path).name or "N/A"
    
    def _find_script_file(self, script_name: str) -> Optional[Path]:
        """Find the actual script file from script name"""
        if script_name == "N/A":
            return None
        
        # Look for exact match first
        for script_file in self.all_scripts:
            if script_file.name == script_name:
                return script_file
        
        # Look for partial match
        for script_file in self.all_scripts:
            if script_name in script_file.name or script_file.name in script_name:
                return script_file
        
        return None
    
    def _analyze_script_comprehensive(self, script_file: Path) -> Dict[str, Any]:
        """Perform comprehensive analysis of a script file"""
        analysis = {
            'input_tables': [],
            'output_tables': [],
            'transformations': [],
            'business_logic': [],
            'data_sources': [],
            'data_targets': []
        }
        
        try:
            with open(script_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            if script_file.suffix.lower() == '.py':
                analysis.update(self._analyze_python_script_comprehensive(content, script_file))
            elif script_file.suffix.lower() == '.pig':
                analysis.update(self._analyze_pig_script_comprehensive(content, script_file))
            elif script_file.suffix.lower() == '.sql':
                analysis.update(self._analyze_sql_script_comprehensive(content, script_file))
            elif script_file.suffix.lower() == '.sh':
                analysis.update(self._analyze_shell_script_comprehensive(content, script_file))
                
        except Exception as e:
            print(f"   ⚠️ Error analyzing script {script_file}: {e}")
        
        return analysis
    
    def _analyze_python_script_comprehensive(self, content: str, script_file: Path) -> Dict[str, Any]:
        """Comprehensive analysis of Python/PySpark scripts"""
        analysis = {
            'input_tables': [],
            'output_tables': [],
            'transformations': [],
            'business_logic': [],
            'data_sources': [],
            'data_targets': []
        }
        
        # Extract table references with more patterns
        table_patterns = [
            r'spark\.read\.table\(["\']([^"\']+)["\']',  # spark.read.table('table')
            r'spark\.read\.format\(["\']([^"\']+)["\']',  # spark.read.format('parquet')
            r'\.load\(["\']([^"\']+)["\']',  # .load('/path/to/data')
            r'\.saveAsTable\(["\']([^"\']+)["\']',  # .saveAsTable('table')
            r'\.write\.mode\(["\']([^"\']+)["\']',  # .write.mode('overwrite')
            r'\.option\(["\']path["\'],\s*["\']([^"\']+)["\']',  # .option('path', '/path')
            r'\.parquet\(["\']([^"\']+)["\']',  # .parquet('/path')
            r'\.csv\(["\']([^"\']+)["\']',  # .csv('/path')
            r'\.json\(["\']([^"\']+)["\']',  # .json('/path')
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if '/hdfs/' in match.lower() or '/data/' in match.lower():
                    table_name = self._extract_table_name_from_path(match)
                    if table_name:
                        analysis['input_tables'].append(table_name)
                        analysis['data_sources'].append(match)
        
        # Extract transformations with more detail
        transform_patterns = [
            r'\.select\(([^)]+)\)',  # .select(columns)
            r'\.filter\(([^)]+)\)',  # .filter(condition)
            r'\.join\(([^)]+)\)',  # .join(other_df)
            r'\.groupBy\(([^)]+)\)',  # .groupBy(columns)
            r'\.agg\(([^)]+)\)',  # .agg(functions)
            r'\.withColumn\(["\']([^"\']+)["\']',  # .withColumn('new_col', ...)
            r'\.drop\(["\']([^"\']+)["\']',  # .drop('col')
            r'\.orderBy\(([^)]+)\)',  # .orderBy(columns)
        ]
        
        for pattern in transform_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                analysis['transformations'].append(match.strip())
        
        # Extract business logic
        business_patterns = [
            r'# Business[:\s]+([^\n]+)',  # Business comments
            r'# Logic[:\s]+([^\n]+)',  # Logic comments
            r'# Rule[:\s]+([^\n]+)',  # Rule comments
            r'# TODO[:\s]+([^\n]+)',  # TODO comments
            r'# NOTE[:\s]+([^\n]+)',  # NOTE comments
        ]
        
        for pattern in business_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                analysis['business_logic'].append(match.strip())
        
        return analysis
    
    def _analyze_pig_script_comprehensive(self, content: str, script_file: Path) -> Dict[str, Any]:
        """Comprehensive analysis of Pig scripts"""
        analysis = {
            'input_tables': [],
            'output_tables': [],
            'transformations': [],
            'business_logic': [],
            'data_sources': [],
            'data_targets': []
        }
        
        # Extract LOAD statements
        load_matches = re.findall(r'LOAD\s+["\']([^"\']+)["\']', content, re.IGNORECASE)
        for match in load_matches:
            analysis['input_tables'].append(match)
            analysis['data_sources'].append(match)
        
        # Extract STORE statements
        store_matches = re.findall(r'STORE\s+(\w+)\s+INTO\s+["\']([^"\']+)["\']', content, re.IGNORECASE)
        for var, path in store_matches:
            analysis['output_tables'].append(path)
            analysis['data_targets'].append(path)
        
        # Extract transformations
        transform_patterns = [
            r'(\w+)\s*=\s*FOREACH\s+([^;]+)',  # FOREACH transformations
            r'(\w+)\s*=\s*FILTER\s+([^;]+)',  # FILTER transformations
            r'(\w+)\s*=\s*JOIN\s+([^;]+)',  # JOIN transformations
            r'(\w+)\s*=\s*GROUP\s+([^;]+)',  # GROUP transformations
            r'(\w+)\s*=\s*DISTINCT\s+([^;]+)',  # DISTINCT transformations
            r'(\w+)\s*=\s*ORDER\s+([^;]+)',  # ORDER transformations
        ]
        
        for pattern in transform_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                analysis['transformations'].append(f"{match[0]} = {match[1]}")
        
        return analysis
    
    def _analyze_sql_script_comprehensive(self, content: str, script_file: Path) -> Dict[str, Any]:
        """Comprehensive analysis of SQL scripts"""
        analysis = {
            'input_tables': [],
            'output_tables': [],
            'transformations': [],
            'business_logic': [],
            'data_sources': [],
            'data_targets': []
        }
        
        # Extract table references
        table_patterns = [
            r'FROM\s+(\w+)',  # FROM table
            r'JOIN\s+(\w+)',  # JOIN table
            r'INTO\s+(\w+)',  # INSERT INTO table
            r'CREATE\s+TABLE\s+(\w+)',  # CREATE TABLE
            r'UPDATE\s+(\w+)',  # UPDATE table
            r'DELETE\s+FROM\s+(\w+)',  # DELETE FROM table
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                analysis['input_tables'].append(match)
        
        # Extract transformations
        transform_patterns = [
            r'SELECT\s+([^FROM]+)',  # SELECT columns
            r'WHERE\s+([^GROUP|ORDER|HAVING]+)',  # WHERE conditions
            r'GROUP\s+BY\s+([^HAVING|ORDER]+)',  # GROUP BY
            r'HAVING\s+([^ORDER]+)',  # HAVING conditions
            r'ORDER\s+BY\s+([^;]+)',  # ORDER BY
        ]
        
        for pattern in transform_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                analysis['transformations'].append(match.strip())
        
        return analysis
    
    def _analyze_shell_script_comprehensive(self, content: str, script_file: Path) -> Dict[str, Any]:
        """Comprehensive analysis of Shell scripts"""
        analysis = {
            'input_tables': [],
            'output_tables': [],
            'transformations': [],
            'business_logic': [],
            'data_sources': [],
            'data_targets': []
        }
        
        # Extract file operations
        file_patterns = [
            r'cp\s+([^\s]+)\s+([^\s]+)',  # cp source dest
            r'mv\s+([^\s]+)\s+([^\s]+)',  # mv source dest
            r'hdfs\s+dfs\s+-cp\s+([^\s]+)\s+([^\s]+)',  # hdfs dfs -cp
            r'hdfs\s+dfs\s+-mv\s+([^\s]+)\s+([^\s]+)',  # hdfs dfs -mv
            r'hdfs\s+dfs\s+-put\s+([^\s]+)\s+([^\s]+)',  # hdfs dfs -put
            r'hdfs\s+dfs\s+-get\s+([^\s]+)\s+([^\s]+)',  # hdfs dfs -get
        ]
        
        for pattern in file_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                analysis['data_sources'].append(match[0])
                analysis['data_targets'].append(match[1])
        
        return analysis
    
    def _extract_table_name_from_path(self, path: str) -> str:
        """Extract table name from HDFS path"""
        # Extract table name from common HDFS patterns
        path_lower = path.lower()
        
        if '/hdfs/' in path_lower:
            parts = path.split('/')
            for i, part in enumerate(parts):
                if part.lower() == 'hdfs' and i + 1 < len(parts):
                    return parts[i + 1]
        
        if '/data/' in path_lower:
            parts = path.split('/')
            for i, part in enumerate(parts):
                if part.lower() == 'data' and i + 1 < len(parts):
                    return parts[i + 1]
        
        # Fallback to last part of path
        return Path(path).name
    
    def _determine_script_business_purpose(self, action_name: str, script_path: str) -> str:
        """Determine business purpose from action name and script path"""
        name_lower = action_name.lower()
        path_lower = (script_path or "").lower()
        
        if any(pattern in name_lower for pattern in ['notification', 'email', 'alert']):
            return 'notification'
        elif any(pattern in name_lower for pattern in ['audit', 'log', 'track']):
            return 'audit'
        elif any(pattern in name_lower for pattern in ['parse', 'extract', 'read']):
            return 'parse'
        elif any(pattern in name_lower for pattern in ['generate', 'create', 'build']):
            return 'generate'
        elif any(pattern in name_lower for pattern in ['append', 'insert', 'load']):
            return 'append'
        elif any(pattern in name_lower for pattern in ['check', 'validate', 'verify']):
            return 'check'
        elif any(pattern in name_lower for pattern in ['transform', 'convert', 'process']):
            return 'transform'
        elif any(pattern in name_lower for pattern in ['extract', 'pull', 'fetch']):
            return 'extract'
        elif any(pattern in name_lower for pattern in ['reconcile', 'match', 'merge']):
            return 'reconcile'
        elif any(pattern in name_lower for pattern in ['ingest', 'load', 'import']):
            return 'ingest'
        else:
            return 'process'
    
    def _determine_business_domain(self, workflow_name: str, actions: List[Dict]) -> str:
        """Determine business domain from workflow name and actions"""
        name_lower = workflow_name.lower()
        
        if any(pattern in name_lower for pattern in ['chc', 'clinical', 'health']):
            return 'CHC (Clinical Health Center)'
        elif any(pattern in name_lower for pattern in ['patient', 'account', 'acct']):
            return 'Patient Account Management'
        elif any(pattern in name_lower for pattern in ['audit', 'report', 'compliance']):
            return 'Audit and Compliance'
        elif any(pattern in name_lower for pattern in ['sqoop', 'jdbc', 'database']):
            return 'Data Ingestion (Sqoop)'
        elif any(pattern in name_lower for pattern in ['big', 'table', 'large']):
            return 'Big Data Processing'
        elif any(pattern in name_lower for pattern in ['swift', 'payment', 'financial']):
            return 'Swift Processing'
        else:
            return 'General Data Processing'
    
    def _determine_frequency(self, workflow_name: str, parameters: Dict) -> str:
        """Determine execution frequency"""
        name_lower = workflow_name.lower()
        
        if any(pattern in name_lower for pattern in ['daily', 'day']):
            return 'Daily'
        elif any(pattern in name_lower for pattern in ['hourly', 'hour']):
            return 'Hourly'
        elif any(pattern in name_lower for pattern in ['weekly', 'week']):
            return 'Weekly'
        elif any(pattern in name_lower for pattern in ['monthly', 'month']):
            return 'Monthly'
        else:
            return 'Unknown'
    
    def _calculate_workflow_complexity(self, actions: List[Dict]) -> int:
        """Calculate workflow complexity score"""
        return len(actions) * 2 + sum(1 for action in actions if action.get('technology') != 'Unknown')
    
    def _extract_workflow_dependencies(self, workflow_name: str, actions: List[Dict]) -> List[str]:
        """Extract workflow dependencies"""
        dependencies = []
        for action in actions:
            if 'depends' in action.get('name', '').lower():
                dependencies.append(action['name'])
        return dependencies
    
    def _find_all_scripts(self):
        """Find all script files in the repository"""
        script_extensions = ['*.py', '*.pig', '*.sql', '*.sh', '*.jar']
        
        for ext in script_extensions:
            scripts = list(self.hadoop_path.rglob(ext))
            self.all_scripts.extend(scripts)
        
        print(f"   📄 Found {len(self.all_scripts)} total scripts")
    
    def _identify_unused_scripts_comprehensive(self):
        """Identify unused scripts including script-to-script dependencies"""
        used_script_files = set()
        
        # Step 1: Get directly referenced scripts from workflows
        for analysis in self.workflow_analyses:
            for action in analysis.get('actions', []):
                script_name = action.get('script_name', '')
                if script_name != 'N/A':
                    # Find the actual script file
                    for script_file in self.all_scripts:
                        if script_file.name == script_name:
                            used_script_files.add(script_file)
                            break
        
        print(f"   📋 Direct workflow references: {len(used_script_files)} scripts")
        
        # Step 2: Analyze script-to-script dependencies
        print(f"   🔍 Analyzing script-to-script dependencies...")
        script_dependencies = self._analyze_script_dependencies_comprehensive(used_script_files)
        
        # Step 3: Add indirectly used scripts
        indirectly_used = set()
        for script_file in used_script_files:
            self._find_indirect_dependencies_comprehensive(script_file, script_dependencies, indirectly_used)
        
        # Combine direct and indirect usage
        all_used_scripts = used_script_files.union(indirectly_used)
        
        # Identify truly unused scripts
        self.unused_scripts = [script for script in self.all_scripts if script not in all_used_scripts]
        
        print(f"   ✅ Directly used scripts: {len(used_script_files)}")
        print(f"   ✅ Indirectly used scripts: {len(indirectly_used)}")
        print(f"   ✅ Total used scripts: {len(all_used_scripts)}")
        print(f"   ✅ Unused scripts: {len(self.unused_scripts)}")
        
        # Store for reporting
        self.used_scripts = all_used_scripts
        self.script_dependencies = script_dependencies
    
    def _analyze_script_dependencies_comprehensive(self, used_scripts: set) -> Dict[Path, List[Path]]:
        """Analyze script-to-script dependencies by reading script content"""
        dependencies = {}
        
        for script_file in used_scripts:
            try:
                if script_file.suffix.lower() in ['.py', '.pig', '.sql', '.sh']:
                    dependencies[script_file] = self._extract_script_calls_comprehensive(script_file)
            except Exception as e:
                print(f"   ⚠️ Error analyzing dependencies for {script_file}: {e}")
        
        return dependencies
    
    def _extract_script_calls_comprehensive(self, script_file: Path) -> List[Path]:
        """Extract script calls from a script file comprehensively"""
        dependencies = []
        
        try:
            with open(script_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            if script_file.suffix.lower() == '.py':
                # Python script patterns
                patterns = [
                    r'exec\(["\']([^"\']+)["\']',  # exec('script.py')
                    r'os\.system\(["\']([^"\']+)["\']',  # os.system('script.sh')
                    r'subprocess\.call\(["\']([^"\']+)["\']',  # subprocess.call('script.sh')
                    r'subprocess\.run\(["\']([^"\']+)["\']',  # subprocess.run('script.sh')
                    r'import\s+(\w+)',  # import module
                    r'from\s+(\w+)\s+import',  # from module import
                    r'\.py["\']',  # .py files
                ]
                
            elif script_file.suffix.lower() == '.pig':
                # Pig script patterns
                patterns = [
                    r'exec\s+["\']([^"\']+)["\']',  # exec 'script.pig'
                    r'run\s+["\']([^"\']+)["\']',  # run 'script.pig'
                    r'\.pig["\']',  # .pig files
                ]
                
            elif script_file.suffix.lower() == '.sql':
                # SQL script patterns
                patterns = [
                    r'source\s+["\']([^"\']+)["\']',  # source 'script.sql'
                    r'\.sql["\']',  # .sql files
                ]
                
            elif script_file.suffix.lower() == '.sh':
                # Shell script patterns
                patterns = [
                    r'\.\s+([^"\s]+\.sh)',  # . script.sh
                    r'source\s+([^"\s]+\.sh)',  # source script.sh
                    r'bash\s+([^"\s]+\.sh)',  # bash script.sh
                    r'sh\s+([^"\s]+\.sh)',  # sh script.sh
                    r'exec\s+([^"\s]+\.sh)',  # exec script.sh
                    r'\.sh["\']',  # .sh files
                ]
            
            else:
                patterns = []
            
            # Extract script references
            for pattern in patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    # Clean up the match
                    script_ref = match.strip().strip('"\'')
                    
                    # Skip if it's not a script file
                    if not any(script_ref.endswith(ext) for ext in ['.py', '.pig', '.sql', '.sh']):
                        continue
                    
                    # Find the actual script file
                    found_script = self._find_script_file(script_ref)
                    if found_script and found_script not in dependencies:
                        dependencies.append(found_script)
            
        except Exception as e:
            print(f"   ⚠️ Error reading script {script_file}: {e}")
        
        return dependencies
    
    def _find_indirect_dependencies_comprehensive(self, script_file: Path, dependencies: Dict[Path, List[Path]], visited: set):
        """Recursively find indirect dependencies"""
        if script_file in visited:
            return
        
        visited.add(script_file)
        
        if script_file in dependencies:
            for dep_script in dependencies[script_file]:
                if dep_script not in visited:
                    self._find_indirect_dependencies_comprehensive(dep_script, dependencies, visited)
    
    def _analyze_business_domains(self, workflow_analyses: List[Dict[str, Any]]):
        """Analyze business domains"""
        domain_counts = defaultdict(int)
        for analysis in workflow_analyses:
            domain_counts[analysis['business_domain']] += 1
        
        print(f"   🏢 Business domains identified: {len(domain_counts)}")
        for domain, count in sorted(domain_counts.items()):
            print(f"      {domain}: {count} workflows")
    
    def _create_pipeline_flows(self):
        """Create pipeline flow analysis"""
        print(f"   🔄 Pipeline flows created")
    
    def _create_execution_steps(self):
        """Create execution step analysis"""
        print(f"   ⚡ Execution steps created")
    
    def _analyze_technology_flows(self):
        """Analyze technology flows"""
        print(f"   🔧 Technology flows analyzed")
    
    def _create_detailed_data_flows(self):
        """Create detailed data flow analysis"""
        print(f"   📊 Detailed data flows created")
    
    def _generate_comprehensive_excel_report(self) -> str:
        """Generate comprehensive Excel report"""
        repo_name = self.hadoop_path.name
        output_file = f"comprehensive_hadoop_analysis_{repo_name}.xlsx"
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: Pipeline Flows
            pipeline_data = []
            for analysis in self.workflow_analyses:
                pipeline_data.append({
                    'Workflow Name': analysis['name'],
                    'Frequency': analysis['frequency'],
                    'Execution Order': analysis['workflow_execution_order'],
                    'Total Actions': analysis['total_actions'],
                    'Technologies': str(analysis['technologies']),
                    'Business Domain': analysis['business_domain'],
                    'Input Tables': ', '.join(analysis['input_tables']) if analysis['input_tables'] else 'N/A',
                    'Output Tables': ', '.join(analysis['output_tables']) if analysis['output_tables'] else 'N/A',
                    'Complexity Score': analysis['complexity_score'],
                    'File Path': analysis['file']
                })
            
            df_pipeline = pd.DataFrame(pipeline_data)
            df_pipeline.to_excel(writer, sheet_name='Pipeline Flows', index=False)
            
            # Sheet 2: Execution Steps
            execution_data = []
            for analysis in self.workflow_analyses:
                for i, action in enumerate(analysis.get('actions', []), 1):
                    execution_data.append({
                        'Workflow': analysis['name'],
                        'Step Order': i,
                        'Action Name': action['name'],
                        'Technology': action['technology'],
                        'Script Name': action['script_name'],
                        'Business Purpose': action['business_purpose'],
                        'Input Tables': ', '.join(action.get('input_tables', [])),
                        'Output Tables': ', '.join(action.get('output_tables', [])),
                        'Data Sources': ', '.join(action.get('data_sources', [])),
                        'Data Targets': ', '.join(action.get('data_targets', [])),
                        'Transformations': ', '.join(action.get('transformations', [])),
                        'Business Logic': ', '.join(action.get('business_logic', []))
                    })
            
            df_execution = pd.DataFrame(execution_data)
            df_execution.to_excel(writer, sheet_name='Execution Steps', index=False)
            
            # Sheet 3: Technology Flows
            tech_data = defaultdict(int)
            for analysis in self.workflow_analyses:
                for tech, count in analysis['technologies'].items():
                    tech_data[tech] += count
            
            df_tech = pd.DataFrame(list(tech_data.items()), columns=['Technology', 'Count'])
            df_tech.to_excel(writer, sheet_name='Technology Flows', index=False)
            
            # Sheet 4: Granular Detail Flows
            detail_data = []
            for analysis in self.workflow_analyses:
                for action in analysis.get('actions', []):
                    if action.get('script_name') != 'N/A':
                        detail_data.append({
                            'Workflow': analysis['name'],
                            'Action': action['name'],
                            'Script Name': action['script_name'],
                            'Technology': action['technology'],
                            'Business Purpose': action['business_purpose'],
                            'Input Tables': ', '.join(action.get('input_tables', [])),
                            'Output Tables': ', '.join(action.get('output_tables', [])),
                            'Data Sources': ', '.join(action.get('data_sources', [])),
                            'Data Targets': ', '.join(action.get('data_targets', [])),
                            'Transformations': ', '.join(action.get('transformations', [])),
                            'Business Logic': ', '.join(action.get('business_logic', []))
                        })
            
            df_detail = pd.DataFrame(detail_data)
            df_detail.to_excel(writer, sheet_name='Granular Detail Flows', index=False)
            
            # Sheet 5: Script Usage Analysis
            script_data = []
            for script_file in self.all_scripts:
                is_directly_used = any(script_file.name == action.get('script_name', '') 
                                    for analysis in self.workflow_analyses 
                                    for action in analysis.get('actions', []))
                is_indirectly_used = script_file in self.used_scripts and not is_directly_used
                is_used = script_file in self.used_scripts
                
                # Get dependencies for this script
                dependencies = []
                if script_file in getattr(self, 'script_dependencies', {}):
                    dependencies = [dep.name for dep in self.script_dependencies[script_file]]
                
                script_data.append({
                    'Script Name': script_file.name,
                    'File Path': str(script_file),
                    'Technology': script_file.suffix[1:].upper() if script_file.suffix else 'Unknown',
                    'Used': 'Yes' if is_used else 'No',
                    'Usage Type': 'Direct' if is_directly_used else ('Indirect' if is_indirectly_used else 'Unused'),
                    'Dependencies': ', '.join(dependencies) if dependencies else 'None',
                    'Size (bytes)': script_file.stat().st_size if script_file.exists() else 0
                })
            
            df_scripts = pd.DataFrame(script_data)
            df_scripts.to_excel(writer, sheet_name='Script Usage Analysis', index=False)
            
            # Sheet 6: Script Dependencies
            if hasattr(self, 'script_dependencies') and self.script_dependencies:
                dep_data = []
                for script_file, dependencies in self.script_dependencies.items():
                    for dep_script in dependencies:
                        dep_data.append({
                            'Source Script': script_file.name,
                            'Source Path': str(script_file),
                            'Dependency Script': dep_script.name,
                            'Dependency Path': str(dep_script),
                            'Dependency Type': 'Script Call'
                        })
                
                if dep_data:
                    df_deps = pd.DataFrame(dep_data)
                    df_deps.to_excel(writer, sheet_name='Script Dependencies', index=False)
        
        print(f"   📊 Comprehensive Excel report generated: {output_file}")
        return output_file
    
    def _print_comprehensive_summary(self):
        """Print comprehensive analysis summary"""
        print("\n📊 Comprehensive Analysis Results!")
        print("=" * 60)
        print(f"   Total Workflows: {len(self.workflow_analyses)}")
        print(f"   Total Scripts: {len(self.all_scripts)}")
        print(f"   Used Scripts: {len(self.used_scripts)}")
        print(f"   Unused Scripts: {len(self.unused_scripts)}")
        print(f"   Usage: {(len(self.used_scripts) / len(self.all_scripts) * 100):.1f}%" if self.all_scripts else "   Usage: 0%")
        
        # Show script dependency analysis
        if hasattr(self, 'script_dependencies') and self.script_dependencies:
            total_deps = sum(len(deps) for deps in self.script_dependencies.values())
            print(f"   Script Dependencies Found: {total_deps}")
            print(f"   Scripts with Dependencies: {len(self.script_dependencies)}")
        
        # Technology breakdown
        tech_counts = defaultdict(int)
        for analysis in self.workflow_analyses:
            for tech, count in analysis['technologies'].items():
                tech_counts[tech] += count
        
        print(f"\n🔧 Technology Breakdown:")
        for tech, count in sorted(tech_counts.items()):
            print(f"   {tech}: {count} scripts")
        
        # Business domains
        domain_counts = defaultdict(int)
        for analysis in self.workflow_analyses:
            domain_counts[analysis['business_domain']] += 1
        
        print(f"\n🏢 Business Domains:")
        for domain, count in sorted(domain_counts.items()):
            print(f"   {domain}: {count} workflows")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Single repo: python comprehensive_hadoop_analyzer.py <hadoop_repo_path>")
        print("  Multiple repos: python comprehensive_hadoop_analyzer.py <repo1> <repo2> <repo3> ...")
        print("  From file: python comprehensive_hadoop_analyzer.py --file <path_to_repo_list.txt>")
        print("\nExamples:")
        print("  python comprehensive_hadoop_analyzer.py /path/to/app-cdd")
        print("  python comprehensive_hadoop_analyzer.py /path/to/app-cdd /path/to/app-data-ingestion")
        print("  python comprehensive_hadoop_analyzer.py --file repo_list.txt")
        sys.exit(1)
    
    # Check if using file input
    if sys.argv[1] == '--file':
        if len(sys.argv) != 3:
            print("Usage: python comprehensive_hadoop_analyzer.py --file <path_to_repo_list.txt>")
            sys.exit(1)
        
        repo_list_file = sys.argv[2]
        try:
            with open(repo_list_file, 'r') as f:
                repo_paths = [line.strip() for line in f.readlines() if line.strip() and not line.startswith('#')]
        except FileNotFoundError:
            print(f"❌ Error: File '{repo_list_file}' not found")
            sys.exit(1)
    else:
        # Multiple repo paths from command line
        repo_paths = sys.argv[1:]
    
    print(f"🚀 Analyzing {len(repo_paths)} repositories...")
    print("=" * 60)
    
    successful_analyses = []
    failed_analyses = []
    
    for i, repo_path in enumerate(repo_paths, 1):
        print(f"\n📁 [{i}/{len(repo_paths)}] Analyzing: {repo_path}")
        print("-" * 50)
        
        try:
            analyzer = ComprehensiveHadoopAnalyzer(repo_path)
            output_file = analyzer.analyze()
            successful_analyses.append((repo_path, output_file))
            print(f"✅ [{i}/{len(repo_paths)}] Completed: {output_file}")
            
        except Exception as e:
            failed_analyses.append((repo_path, str(e)))
            print(f"❌ [{i}/{len(repo_paths)}] Failed: {e}")
    
    # Print summary
    print(f"\n🎯 COMPREHENSIVE ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"✅ Successful: {len(successful_analyses)}")
    print(f"❌ Failed: {len(failed_analyses)}")
    
    if successful_analyses:
        print(f"\n📊 Generated Reports:")
        for repo_path, output_file in successful_analyses:
            repo_name = Path(repo_path).name
            print(f"   📄 {repo_name}: {output_file}")
    
    if failed_analyses:
        print(f"\n⚠️ Failed Analyses:")
        for repo_path, error in failed_analyses:
            repo_name = Path(repo_path).name
            print(f"   ❌ {repo_name}: {error}")
    
    print(f"\n📋 Each comprehensive report includes:")
    print("   1. Pipeline Flows - High-level workflow overview")
    print("   2. Execution Steps - Detailed action-by-action analysis")
    print("   3. Technology Flows - Technology breakdown and usage")
    print("   4. Granular Detail Flows - Deep script analysis with transformations")
    print("   5. Script Usage Analysis - Direct vs indirect script usage")
    print("   6. Script Dependencies - Script-to-script call relationships")
    
    # Exit with error code if any failed
    if failed_analyses:
        sys.exit(1)

if __name__ == "__main__":
    main()
