#!/usr/bin/env python3
"""
Hadoop Oozie Workflow Analyzer
==============================

This script analyzes Hadoop repositories focusing on Oozie workflows to understand:
- Which Oozie workflows run first, second, etc.
- What scripts each workflow uses and in which order
- Source and target tables for each script
- Used vs unused scripts
- Complete pipeline flow

Output: Comprehensive Excel report with multiple sheets
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
    """Represents an Oozie workflow"""
    name: str
    file_path: str
    execution_order: int
    frequency: str
    total_actions: int
    actions: List[Dict[str, Any]]
    technologies: Dict[str, int]
    input_tables: List[str]
    output_tables: List[str]

@dataclass
class ScriptInfo:
    """Represents a script with detailed information"""
    name: str
    path: str
    technology: str
    is_used: bool
    used_in_workflows: List[str]
    execution_order: int
    input_tables: List[str]
    output_tables: List[str]
    business_logic: str
    dependencies: List[str]

@dataclass
class PipelineFlow:
    """Represents the complete pipeline flow"""
    workflow_name: str
    step_number: int
    action_name: str
    script_name: str
    technology: str
    input_tables: str
    output_tables: str
    business_purpose: str

class HadoopOozieAnalyzer:
    """Analyzes Hadoop Oozie workflows and scripts"""
    
    def __init__(self, hadoop_repo_path: str):
        self.hadoop_repo_path = Path(hadoop_repo_path)
        self.workflows = []
        self.scripts = []
        self.used_scripts = set()
        self.unused_scripts = set()
        
    def analyze_hadoop_repository(self) -> Dict[str, Any]:
        """Perform complete Hadoop repository analysis"""
        print("🚀 Hadoop Oozie Workflow Analyzer")
        print("=" * 60)
        
        # Step 1: Find all Oozie workflows
        print("🔍 Step 1: Finding Oozie workflows...")
        oozie_workflows = self._find_oozie_workflows()
        print(f"   Found {len(oozie_workflows)} Oozie workflow files")
        
        # Step 2: Analyze each workflow
        print("🔍 Step 2: Analyzing workflow execution order...")
        workflow_analyses = self._analyze_workflows(oozie_workflows)
        
        # Step 3: Find all scripts
        print("🔍 Step 3: Finding all scripts...")
        all_scripts = self._find_all_scripts()
        print(f"   Found {len(all_scripts)} script files")
        
        # Step 4: Identify used vs unused scripts
        print("🔍 Step 4: Identifying used vs unused scripts...")
        used_scripts, unused_scripts = self._identify_used_scripts(all_scripts, workflow_analyses)
        
        # Step 5: Analyze script details
        print("🔍 Step 5: Analyzing script details...")
        script_analyses = self._analyze_script_details(used_scripts, workflow_analyses)
        
        # Step 6: Create pipeline flow
        print("🔍 Step 6: Creating pipeline flow...")
        pipeline_flow = self._create_pipeline_flow(workflow_analyses)
        
        return {
            'workflows': workflow_analyses,
            'scripts': script_analyses,
            'used_scripts': used_scripts,
            'unused_scripts': unused_scripts,
            'pipeline_flow': pipeline_flow,
            'summary': self._create_summary(workflow_analyses, script_analyses, used_scripts, unused_scripts)
        }
    
    def _find_oozie_workflows(self) -> List[Path]:
        """Find all Oozie workflow files"""
        # Look for XML files in coordinators and workflows directories
        patterns = [
            "**/coordinators/*.xml",
            "**/workflows/**/*.xml"
        ]
        
        workflow_files = []
        for pattern in patterns:
            files = list(self.hadoop_repo_path.glob(pattern))
            workflow_files.extend(files)
        
        return workflow_files
    
    def _analyze_workflows(self, oozie_workflows: List[Path]) -> List[OozieWorkflow]:
        """Analyze all Oozie workflows"""
        workflow_analyses = []
        
        for i, workflow_file in enumerate(oozie_workflows):
            try:
                analysis = self._analyze_single_workflow(workflow_file, i + 1)
                if analysis:
                    workflow_analyses.append(analysis)
                    print(f"   ✅ Analyzed: {analysis.name}")
            except Exception as e:
                print(f"   ❌ Error analyzing {workflow_file}: {e}")
        
        return workflow_analyses
    
    def _analyze_single_workflow(self, workflow_file: Path, execution_order: int) -> Optional[OozieWorkflow]:
        """Analyze a single Oozie workflow"""
        try:
            tree = ET.parse(workflow_file)
            root = tree.getroot()
            
            workflow_name = root.get('name', workflow_file.stem)
            
            # Extract frequency if it's a coordinator
            frequency = "Unknown"
            if 'coordinator' in workflow_file.name.lower():
                frequency = root.get('frequency', 'Unknown')
            
            # Extract actions
            actions = []
            technologies = {}
            input_tables = []
            output_tables = []
            
            for action in root.findall('.//{uri:oozie:workflow:0.5}action'):
                action_info = self._parse_action(action, workflow_file)
                if action_info:
                    actions.append(action_info)
                    
                    # Count technologies
                    tech = action_info.get('technology', 'unknown')
                    technologies[tech] = technologies.get(tech, 0) + 1
                    
                    # Collect input/output tables
                    input_tables.extend(action_info.get('input_tables', []))
                    output_tables.extend(action_info.get('output_tables', []))
            
            return OozieWorkflow(
                name=workflow_name,
                file_path=str(workflow_file),
                execution_order=execution_order,
                frequency=frequency,
                total_actions=len(actions),
                actions=actions,
                technologies=technologies,
                input_tables=list(set(input_tables)),
                output_tables=list(set(output_tables))
            )
            
        except Exception as e:
            print(f"   ⚠️ Error parsing {workflow_file}: {e}")
            return None
    
    def _parse_action(self, action_elem, workflow_file: Path) -> Optional[Dict[str, Any]]:
        """Parse an individual Oozie action"""
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
        
        return {
            'action_name': action_name,
            'action_type': action_type,
            'technology': technology,
            'script_path': script_path,
            'script_name': Path(script_path).name if script_path else 'N/A',
            'input_tables': input_tables,
            'output_tables': output_tables,
            'business_purpose': self._infer_business_purpose(action_name, script_path)
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
        
        # Look for spark.read patterns
        patterns = [
            r'spark\.read\.parquet\(["\']([^"\']+)["\']',
            r'spark\.read\.table\(["\']([^"\']+)["\']',
            r'spark\.read\.format\(["\']([^"\']+)["\']',
            r'\.read\.parquet\(["\']([^"\']+)["\']',
            r'\.read\.table\(["\']([^"\']+)["\']'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                table_name = self._extract_table_name_from_path(match)
                if table_name:
                    tables.append(table_name)
        
        return list(set(tables))
    
    def _extract_spark_output_tables(self, content: str) -> List[str]:
        """Extract output tables from Spark/PySpark code"""
        tables = []
        
        # Look for write patterns
        patterns = [
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.parquet\(["\']([^"\']+)["\']',
            r'\.write\.parquet\(["\']([^"\']+)["\']',
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.table\(["\']([^"\']+)["\']',
            r'\.write\.table\(["\']([^"\']+)["\']'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    path = match[1] if len(match) > 1 else match[0]
                else:
                    path = match
                table_name = self._extract_table_name_from_path(path)
                if table_name:
                    tables.append(table_name)
        
        return list(set(tables))
    
    def _extract_pig_input_tables(self, content: str) -> List[str]:
        """Extract input tables from Pig code"""
        tables = []
        
        # Look for LOAD statements
        pattern = r'LOAD\s+[\'"]([^\'"]+)[\'"]'
        matches = re.findall(pattern, content, re.IGNORECASE)
        for match in matches:
            table_name = self._extract_table_name_from_path(match)
            if table_name:
                tables.append(table_name)
        
        return list(set(tables))
    
    def _extract_pig_output_tables(self, content: str) -> List[str]:
        """Extract output tables from Pig code"""
        tables = []
        
        # Look for STORE statements
        pattern = r'STORE\s+\w+\s+INTO\s+[\'"]([^\'"]+)[\'"]'
        matches = re.findall(pattern, content, re.IGNORECASE)
        for match in matches:
            table_name = self._extract_table_name_from_path(match)
            if table_name:
                tables.append(table_name)
        
        return list(set(tables))
    
    def _extract_hive_input_tables(self, content: str) -> List[str]:
        """Extract input tables from Hive SQL"""
        tables = []
        
        # Look for FROM clauses and JOINs
        patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'LEFT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'RIGHT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'INNER\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.extend(matches)
        
        return list(set(tables))
    
    def _extract_hive_output_tables(self, content: str) -> List[str]:
        """Extract output tables from Hive SQL"""
        tables = []
        
        # Look for INSERT INTO and CREATE TABLE
        patterns = [
            r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'CREATE\s+EXTERNAL\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.extend(matches)
        
        return list(set(tables))
    
    def _extract_shell_input_tables(self, content: str) -> List[str]:
        """Extract input tables from Shell scripts"""
        tables = []
        
        # Look for file paths that might be tables
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
        return self._extract_shell_input_tables(content)  # Same logic for shell scripts
    
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
    
    def _find_all_scripts(self) -> List[Path]:
        """Find all script files in the repository"""
        script_extensions = ['.py', '.pig', '.sql', '.sh', '.scala']
        script_files = []
        
        for ext in script_extensions:
            files = list(self.hadoop_repo_path.glob(f"**/*{ext}"))
            script_files.extend(files)
        
        return script_files
    
    def _identify_used_scripts(self, all_scripts: List[Path], workflow_analyses: List[OozieWorkflow]) -> Tuple[Set[Path], Set[Path]]:
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
        unused_scripts = set(all_scripts) - used_scripts
        
        return used_scripts, unused_scripts
    
    def _analyze_script_details(self, used_scripts: Set[Path], workflow_analyses: List[OozieWorkflow]) -> List[ScriptInfo]:
        """Analyze details of used scripts"""
        script_analyses = []
        
        for script_file in used_scripts:
            try:
                # Determine technology
                technology = self._determine_technology(script_file)
                
                # Find which workflows use this script
                used_in_workflows = []
                execution_order = 999
                
                for workflow in workflow_analyses:
                    for i, action in enumerate(workflow.actions):
                        if script_file.name in action.get('script_path', ''):
                            used_in_workflows.append(workflow.name)
                            execution_order = min(execution_order, i + 1)
                
                # Extract business logic
                business_logic = self._extract_business_logic(script_file)
                
                # Extract input/output tables
                input_tables, output_tables = self._extract_table_mappings(script_file)
                
                script_info = ScriptInfo(
                    name=script_file.name,
                    path=str(script_file),
                    technology=technology,
                    is_used=True,
                    used_in_workflows=used_in_workflows,
                    execution_order=execution_order,
                    input_tables=input_tables,
                    output_tables=output_tables,
                    business_logic=business_logic,
                    dependencies=[]
                )
                
                script_analyses.append(script_info)
                
            except Exception as e:
                print(f"   ⚠️ Error analyzing {script_file}: {e}")
        
        return script_analyses
    
    def _determine_technology(self, script_file: Path) -> str:
        """Determine technology based on file extension"""
        ext = script_file.suffix.lower()
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
    
    def _extract_business_logic(self, script_file: Path) -> str:
        """Extract business logic from script"""
        try:
            content = script_file.read_text()
            # Simple extraction - look for comments and function names
            lines = content.split('\n')
            logic_lines = []
            
            for line in lines[:50]:  # First 50 lines
                line = line.strip()
                if line.startswith('#') or line.startswith('--') or line.startswith('//'):
                    logic_lines.append(line)
                elif 'def ' in line or 'function' in line:
                    logic_lines.append(line)
            
            return ' '.join(logic_lines[:5])  # First 5 logic lines
        except:
            return f"Script: {script_file.name}"
    
    def _extract_table_mappings(self, script_file: Path) -> Tuple[List[str], List[str]]:
        """Extract input/output table mappings from script"""
        try:
            content = script_file.read_text()
            input_tables = []
            output_tables = []
            
            # Look for table patterns
            if script_file.suffix == '.py':
                # Spark/PySpark patterns
                input_patterns = [
                    r'spark\.read\.parquet\(["\']([^"\']+)["\']',
                    r'spark\.read\.table\(["\']([^"\']+)["\']',
                    r'\.read\.parquet\(["\']([^"\']+)["\']'
                ]
                output_patterns = [
                    r'\.write\.parquet\(["\']([^"\']+)["\']',
                    r'\.write\.table\(["\']([^"\']+)["\']'
                ]
                
                for pattern in input_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        table_name = self._extract_table_name_from_path(match)
                        if table_name:
                            input_tables.append(table_name)
                
                for pattern in output_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        table_name = self._extract_table_name_from_path(match)
                        if table_name:
                            output_tables.append(table_name)
            
            return list(set(input_tables)), list(set(output_tables))
        except:
            return [], []
    
    def _create_pipeline_flow(self, workflow_analyses: List[OozieWorkflow]) -> List[PipelineFlow]:
        """Create complete pipeline flow"""
        pipeline_flow = []
        
        for workflow in workflow_analyses:
            for i, action in enumerate(workflow.actions):
                flow_item = PipelineFlow(
                    workflow_name=workflow.name,
                    step_number=i + 1,
                    action_name=action.get('action_name', ''),
                    script_name=action.get('script_name', ''),
                    technology=action.get('technology', ''),
                    input_tables=', '.join(action.get('input_tables', [])),
                    output_tables=', '.join(action.get('output_tables', [])),
                    business_purpose=action.get('business_purpose', '')
                )
                pipeline_flow.append(flow_item)
        
        return pipeline_flow
    
    def _create_summary(self, workflow_analyses: List[OozieWorkflow], script_analyses: List[ScriptInfo], 
                       used_scripts: Set[Path], unused_scripts: Set[Path]) -> Dict[str, Any]:
        """Create analysis summary"""
        total_workflows = len(workflow_analyses)
        total_scripts = len(used_scripts) + len(unused_scripts)
        used_scripts_count = len(used_scripts)
        unused_scripts_count = len(unused_scripts)
        
        # Technology breakdown
        tech_breakdown = {}
        for script in script_analyses:
            tech = script.technology
            tech_breakdown[tech] = tech_breakdown.get(tech, 0) + 1
        
        # Workflow frequency analysis
        frequency_analysis = {}
        for workflow in workflow_analyses:
            freq = workflow.frequency
            frequency_analysis[freq] = frequency_analysis.get(freq, 0) + 1
        
        return {
            'total_workflows': total_workflows,
            'total_scripts': total_scripts,
            'used_scripts_count': used_scripts_count,
            'unused_scripts_count': unused_scripts_count,
            'technology_breakdown': tech_breakdown,
            'frequency_analysis': frequency_analysis,
            'workflow_execution_order': [w.name for w in sorted(workflow_analyses, key=lambda x: x.execution_order)]
        }
    
    def generate_excel_report(self, analysis: Dict[str, Any], output_file: str = "hadoop_oozie_analysis.xlsx"):
        """Generate comprehensive Excel report"""
        print(f"📊 Generating Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Summary Sheet
            summary_data = []
            summary = analysis['summary']
            summary_data.append({
                'Metric': 'Total Workflows',
                'Value': summary['total_workflows']
            })
            summary_data.append({
                'Metric': 'Total Scripts',
                'Value': summary['total_scripts']
            })
            summary_data.append({
                'Metric': 'Used Scripts',
                'Value': summary['used_scripts_count']
            })
            summary_data.append({
                'Metric': 'Unused Scripts',
                'Value': summary['unused_scripts_count']
            })
            summary_data.append({
                'Metric': 'Usage Percentage',
                'Value': f"{(summary['used_scripts_count'] / summary['total_scripts'] * 100):.1f}%"
            })
            
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Workflows Sheet
            workflows_data = []
            for workflow in analysis['workflows']:
                workflows_data.append({
                    'Execution Order': workflow.execution_order,
                    'Workflow Name': workflow.name,
                    'File Path': workflow.file_path,
                    'Frequency': workflow.frequency,
                    'Total Actions': workflow.total_actions,
                    'Technologies': str(workflow.technologies),
                    'Input Tables': ', '.join(workflow.input_tables),
                    'Output Tables': ', '.join(workflow.output_tables)
                })
            
            pd.DataFrame(workflows_data).to_excel(writer, sheet_name='Workflows', index=False)
            
            # Scripts Sheet
            scripts_data = []
            for script in analysis['scripts']:
                scripts_data.append({
                    'Script Name': script.name,
                    'Path': script.path,
                    'Technology': script.technology,
                    'Is Used': script.is_used,
                    'Used in Workflows': ', '.join(script.used_in_workflows),
                    'Execution Order': script.execution_order,
                    'Input Tables': ', '.join(script.input_tables),
                    'Output Tables': ', '.join(script.output_tables),
                    'Business Logic': script.business_logic
                })
            
            pd.DataFrame(scripts_data).to_excel(writer, sheet_name='Scripts', index=False)
            
            # Pipeline Flow Sheet
            pipeline_data = []
            for flow in analysis['pipeline_flow']:
                pipeline_data.append({
                    'Workflow Name': flow.workflow_name,
                    'Step Number': flow.step_number,
                    'Action Name': flow.action_name,
                    'Script Name': flow.script_name,
                    'Technology': flow.technology,
                    'Input Tables': flow.input_tables,
                    'Output Tables': flow.output_tables,
                    'Business Purpose': flow.business_purpose
                })
            
            pd.DataFrame(pipeline_data).to_excel(writer, sheet_name='Pipeline Flow', index=False)
            
            # Unused Scripts Sheet
            unused_data = []
            for script in analysis['unused_scripts']:
                unused_data.append({
                    'Unused Script': str(script),
                    'Technology': Path(script).suffix,
                    'Size': f"{script.stat().st_size} bytes" if script.exists() else "Unknown"
                })
            
            pd.DataFrame(unused_data).to_excel(writer, sheet_name='Unused Scripts', index=False)
            
            # Technology Breakdown Sheet
            tech_data = []
            for tech, count in analysis['summary']['technology_breakdown'].items():
                tech_data.append({
                    'Technology': tech,
                    'Count': count,
                    'Percentage': f"{(count / analysis['summary']['used_scripts_count'] * 100):.1f}%"
                })
            
            pd.DataFrame(tech_data).to_excel(writer, sheet_name='Technology Breakdown', index=False)
        
        print(f"✅ Excel report generated: {output_file}")

def main():
    """Main function"""
    import sys
    
    print("🚀 Hadoop Oozie Workflow Analyzer")
    print("=" * 60)
    
    # Check command line arguments
    if len(sys.argv) == 2:
        hadoop_repo_path = sys.argv[1]
    else:
        print("Usage: python hadoop_oozie_analyzer.py <hadoop_path>")
        print("\nExample:")
        print("python hadoop_oozie_analyzer.py /path/to/hadoop/repo")
        print("\nOr run with default test path:")
        print("python hadoop_oozie_analyzer.py")
        
        # Use default test path
        hadoop_repo_path = "./OneDrive_1_7-25-2025/Hadoop/app-data-ingestion"
        print(f"\n🔧 Using default test path: {hadoop_repo_path}")
    
    if not hadoop_repo_path:
        print("❌ Please provide Hadoop repository path")
        return
    
    # Initialize analyzer
    analyzer = HadoopOozieAnalyzer(hadoop_repo_path)
    
    # Perform analysis
    analysis = analyzer.analyze_hadoop_repository()
    
    # Generate Excel report
    analyzer.generate_excel_report(analysis)
    
    # Print summary
    summary = analysis['summary']
    print(f"\n📊 Analysis Complete!")
    print(f"   Total Workflows: {summary['total_workflows']}")
    print(f"   Total Scripts: {summary['total_scripts']}")
    print(f"   Used Scripts: {summary['used_scripts_count']}")
    print(f"   Unused Scripts: {summary['unused_scripts_count']}")
    print(f"   Usage: {(summary['used_scripts_count'] / summary['total_scripts'] * 100):.1f}%")
    
    print(f"\n🔧 Technology Breakdown:")
    for tech, count in summary['technology_breakdown'].items():
        print(f"   {tech.upper()}: {count} scripts")
    
    print(f"\n✅ Excel report generated: hadoop_oozie_analysis.xlsx")

if __name__ == "__main__":
    main()
