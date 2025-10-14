import os
import re
import json
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from pathlib import Path
import ast
import sqlparse
from collections import defaultdict
import xml.etree.ElementTree as ET

class EnhancedFlowAnalyzer:
    """
    Enhanced analyzer that understands execution flow, dependencies, and detailed logic
    """
    
    def __init__(self):
        self.hadoop_processes = []
        self.databricks_processes = []
        self.execution_flows = {}
        self.dependencies = {}
        self.business_logic_patterns = {
            'permid': ['permid', 'person_id', 'personid', 'tu_id', 'transunion'],
            'coverage': ['coverage', 'insurance', 'policy', 'payer', 'benefit'],
            'patient': ['patient', 'demographic', 'demographics', 'patientacct'],
            'address': ['address', 'addr', 'location', 'zip', 'city', 'state'],
            'phone': ['phone', 'telephone', 'contact'],
            'family': ['family', 'household', 'member', 'dependent', 'subscriber'],
            'lead': ['lead', 'discovery', 'generation', 'propagation'],
            'validation': ['validate', 'validation', 'check', 'verify'],
            'transformation': ['transform', 'convert', 'map', 'process'],
            'merge': ['merge', 'join', 'combine', 'union'],
            'filter': ['filter', 'where', 'condition', 'criteria'],
            'group': ['group', 'aggregate', 'sum', 'count', 'avg'],
            'sort': ['sort', 'order', 'rank'],
            'dedupe': ['dedupe', 'distinct', 'unique', 'duplicate'],
            'cm2': ['cm2', 'matching', 'pass', 'stage'],
            'bdf': ['bdf', 'batch', 'data', 'file'],
            'prebdf': ['prebdf', 'pre', 'before', 'preparation'],
            'postbdf': ['postbdf', 'post', 'after', 'processing']
        }
    
    def analyze_hadoop_execution_flow(self, hadoop_path):
        """Analyze Hadoop execution flow from workflow files and scripts"""
        print(f"Analyzing Hadoop execution flow: {hadoop_path}")
        
        hadoop_path = Path(hadoop_path)
        
        # First, analyze workflow files to understand execution order
        workflow_files = list(hadoop_path.rglob("*workflow*.xml"))
        for workflow_file in workflow_files:
            self.analyze_oozie_workflow(workflow_file)
        
        # Analyze coordinator files for scheduling
        coordinator_files = list(hadoop_path.rglob("*coordinator*.xml"))
        for coordinator_file in coordinator_files:
            self.analyze_oozie_coordinator(coordinator_file)
        
        # Then analyze individual scripts
        pig_files = list(hadoop_path.rglob("*.pig"))
        for pig_file in pig_files:
            process = self.analyze_pig_script_detailed(pig_file)
            if process:
                self.hadoop_processes.append(process)
        
        py_files = list(hadoop_path.rglob("*.py"))
        for py_file in py_files:
            process = self.analyze_pyspark_script_detailed(py_file)
            if process:
                self.hadoop_processes.append(process)
        
        # Build execution flow
        self.build_hadoop_execution_flow()
        
        print(f"Found {len(self.hadoop_processes)} Hadoop processes")
    
    def analyze_databricks_execution_flow(self, databricks_path):
        """Analyze Databricks execution flow from pipeline files and notebooks"""
        print(f"Analyzing Databricks execution flow: {databricks_path}")
        
        databricks_path = Path(databricks_path)
        
        # Look for pipeline definition files
        pipeline_files = list(databricks_path.rglob("*pipeline*.json"))
        for pipeline_file in pipeline_files:
            self.analyze_databricks_pipeline(pipeline_file)
        
        # Look for workflow files
        workflow_files = list(databricks_path.rglob("*workflow*.json"))
        for workflow_file in workflow_files:
            self.analyze_databricks_workflow(workflow_file)
        
        # Analyze notebooks
        py_files = list(databricks_path.rglob("*.py"))
        for py_file in py_files:
            process = self.analyze_databricks_notebook_detailed(py_file)
            if process:
                self.databricks_processes.append(process)
        
        sql_files = list(databricks_path.rglob("*.sql"))
        for sql_file in sql_files:
            process = self.analyze_databricks_sql_detailed(sql_file)
            if process:
                self.databricks_processes.append(process)
        
        # Build execution flow
        self.build_databricks_execution_flow()
        
        print(f"Found {len(self.databricks_processes)} Databricks processes")
    
    def analyze_oozie_workflow(self, workflow_file):
        """Analyze Oozie workflow XML to understand execution order"""
        try:
            tree = ET.parse(workflow_file)
            root = tree.getroot()
            
            workflow_name = root.get('name', 'Unknown')
            execution_order = []
            
            # Extract start node
            start_node = root.find('.//start')
            if start_node is not None:
                execution_order.append(start_node.get('to'))
            
            # Extract action sequence
            actions = root.findall('.//action')
            for action in actions:
                action_name = action.get('name')
                ok_node = action.find('ok')
                if ok_node is not None:
                    next_action = ok_node.get('to')
                    execution_order.append((action_name, next_action))
            
            # Extract forks and joins
            forks = root.findall('.//fork')
            for fork in forks:
                fork_name = fork.get('name')
                paths = [path.get('start') for path in fork.findall('.//path')]
                execution_order.append(('fork', fork_name, paths))
            
            joins = root.findall('.//join')
            for join in joins:
                join_name = join.get('name')
                execution_order.append(('join', join_name))
            
            self.execution_flows[workflow_name] = {
                'type': 'Oozie Workflow',
                'file': str(workflow_file),
                'execution_order': execution_order
            }
            
        except Exception as e:
            print(f"Error analyzing Oozie workflow {workflow_file}: {e}")
    
    def analyze_oozie_coordinator(self, coordinator_file):
        """Analyze Oozie coordinator XML to understand scheduling"""
        try:
            tree = ET.parse(coordinator_file)
            root = tree.getroot()
            
            coordinator_name = root.get('name', 'Unknown')
            frequency = root.get('frequency', 'Unknown')
            start_time = root.get('start', 'Unknown')
            end_time = root.get('end', 'Unknown')
            
            # Extract workflow path
            workflow_elem = root.find('.//workflow/app-path')
            workflow_path = workflow_elem.text if workflow_elem is not None else 'Unknown'
            
            self.execution_flows[coordinator_name] = {
                'type': 'Oozie Coordinator',
                'file': str(coordinator_file),
                'frequency': frequency,
                'start_time': start_time,
                'end_time': end_time,
                'workflow_path': workflow_path
            }
            
        except Exception as e:
            print(f"Error analyzing Oozie coordinator {coordinator_file}: {e}")
    
    def analyze_databricks_pipeline(self, pipeline_file):
        """Analyze Databricks pipeline JSON to understand execution order"""
        try:
            with open(pipeline_file, 'r', encoding='utf-8') as f:
                pipeline_data = json.load(f)
            
            pipeline_name = pipeline_data.get('name', 'Unknown')
            stages = pipeline_data.get('stages', [])
            
            execution_order = []
            for stage in stages:
                stage_name = stage.get('name', 'Unknown')
                notebooks = stage.get('notebooks', [])
                dependencies = stage.get('dependencies', [])
                
                execution_order.append({
                    'stage': stage_name,
                    'notebooks': notebooks,
                    'dependencies': dependencies
                })
            
            self.execution_flows[pipeline_name] = {
                'type': 'Databricks Pipeline',
                'file': str(pipeline_file),
                'execution_order': execution_order
            }
            
        except Exception as e:
            print(f"Error analyzing Databricks pipeline {pipeline_file}: {e}")
    
    def analyze_databricks_workflow(self, workflow_file):
        """Analyze Databricks workflow JSON"""
        try:
            with open(workflow_file, 'r', encoding='utf-8') as f:
                workflow_data = json.load(f)
            
            workflow_name = workflow_data.get('name', 'Unknown')
            tasks = workflow_data.get('tasks', [])
            
            execution_order = []
            for task in tasks:
                task_name = task.get('task_key', 'Unknown')
                notebook_path = task.get('notebook_task', {}).get('notebook_path', 'Unknown')
                dependencies = task.get('depends_on', [])
                
                execution_order.append({
                    'task': task_name,
                    'notebook': notebook_path,
                    'dependencies': dependencies
                })
            
            self.execution_flows[workflow_name] = {
                'type': 'Databricks Workflow',
                'file': str(workflow_file),
                'execution_order': execution_order
            }
            
        except Exception as e:
            print(f"Error analyzing Databricks workflow {workflow_file}: {e}")
    
    def analyze_pig_script_detailed(self, pig_file):
        """Analyze Pig script with detailed execution flow and comments"""
        try:
            with open(pig_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract comments and section headers
            comments = self.extract_pig_comments(content)
            
            # Extract execution steps
            execution_steps = self.extract_pig_execution_steps(content)
            
            # Extract data flow
            data_flow = self.extract_pig_data_flow_detailed(content)
            
            # Extract business logic from comments
            business_logic = self.extract_business_logic_from_comments(comments)
            
            # Analyze CM2 matching passes if present
            cm2_passes = self.extract_cm2_matching_passes(content)
            
            process = {
                'type': 'Pig Script',
                'name': pig_file.name,
                'path': str(pig_file),
                'relative_path': str(pig_file.relative_to(pig_file.parents[2])),
                'comments': comments,
                'execution_steps': execution_steps,
                'data_flow': data_flow,
                'business_logic': business_logic,
                'cm2_passes': cm2_passes,
                'content_snippet': content[:1000] + "..." if len(content) > 1000 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing Pig script {pig_file}: {e}")
            return None
    
    def analyze_pyspark_script_detailed(self, py_file):
        """Analyze PySpark script with detailed execution flow"""
        try:
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract comments and docstrings
            comments = self.extract_python_comments(content)
            
            # Extract execution steps
            execution_steps = self.extract_pyspark_execution_steps(content)
            
            # Extract data flow
            data_flow = self.extract_pyspark_data_flow_detailed(content)
            
            # Extract business logic
            business_logic = self.extract_business_logic_from_comments(comments)
            
            # Extract function definitions and their purposes
            functions = self.extract_python_functions_detailed(content)
            
            process = {
                'type': 'PySpark Script',
                'name': py_file.name,
                'path': str(py_file),
                'relative_path': str(py_file.relative_to(py_file.parents[2])),
                'comments': comments,
                'execution_steps': execution_steps,
                'data_flow': data_flow,
                'business_logic': business_logic,
                'functions': functions,
                'content_snippet': content[:1000] + "..." if len(content) > 1000 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing PySpark script {py_file}: {e}")
            return None
    
    def analyze_databricks_notebook_detailed(self, py_file):
        """Analyze Databricks notebook with detailed execution flow"""
        try:
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract comments and docstrings
            comments = self.extract_python_comments(content)
            
            # Extract execution steps
            execution_steps = self.extract_databricks_execution_steps(content)
            
            # Extract data flow
            data_flow = self.extract_databricks_data_flow_detailed(content)
            
            # Extract business logic
            business_logic = self.extract_business_logic_from_comments(comments)
            
            # Extract function definitions
            functions = self.extract_python_functions_detailed(content)
            
            process = {
                'type': 'Databricks Notebook',
                'name': py_file.name,
                'path': str(py_file),
                'relative_path': str(py_file.relative_to(py_file.parents[2])),
                'comments': comments,
                'execution_steps': execution_steps,
                'data_flow': data_flow,
                'business_logic': business_logic,
                'functions': functions,
                'content_snippet': content[:1000] + "..." if len(content) > 1000 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing Databricks notebook {py_file}: {e}")
            return None
    
    def analyze_databricks_sql_detailed(self, sql_file):
        """Analyze Databricks SQL with detailed execution flow"""
        try:
            with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract comments
            comments = self.extract_sql_comments(content)
            
            # Extract execution steps
            execution_steps = self.extract_sql_execution_steps(content)
            
            # Extract business logic
            business_logic = self.extract_business_logic_from_comments(comments)
            
            process = {
                'type': 'Databricks SQL',
                'name': sql_file.name,
                'path': str(sql_file),
                'relative_path': str(sql_file.relative_to(sql_file.parents[2])),
                'comments': comments,
                'execution_steps': execution_steps,
                'business_logic': business_logic,
                'content_snippet': content[:1000] + "..." if len(content) > 1000 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing Databricks SQL {sql_file}: {e}")
            return None
    
    def extract_pig_comments(self, content):
        """Extract comments from Pig script"""
        comments = []
        lines = content.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('--') or line.startswith('/*') or line.startswith('*'):
                comments.append(line)
        
        return comments
    
    def extract_python_comments(self, content):
        """Extract comments and docstrings from Python script"""
        comments = []
        lines = content.split('\n')
        
        in_docstring = False
        for line in lines:
            line_stripped = line.strip()
            
            # Check for docstrings
            if '"""' in line_stripped or "'''" in line_stripped:
                in_docstring = not in_docstring
            
            # Check for regular comments
            if line_stripped.startswith('#') and not in_docstring:
                comments.append(line_stripped)
            
            # Add docstring content
            if in_docstring and line_stripped:
                comments.append(line_stripped)
        
        return comments
    
    def extract_sql_comments(self, content):
        """Extract comments from SQL script"""
        comments = []
        lines = content.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('--') or line.startswith('/*'):
                comments.append(line)
        
        return comments
    
    def extract_pig_execution_steps(self, content):
        """Extract execution steps from Pig script"""
        steps = []
        lines = content.split('\n')
        
        current_step = None
        for line in lines:
            line_stripped = line.strip()
            
            # Look for section headers
            if line_stripped.startswith('----'):
                current_step = line_stripped.replace('-', '').strip()
                steps.append({'step': current_step, 'operations': []})
            
            # Look for Pig operations
            elif current_step and ('=' in line_stripped and ('LOAD' in line_stripped or 'FOREACH' in line_stripped or 'FILTER' in line_stripped or 'GROUP' in line_stripped or 'JOIN' in line_stripped or 'STORE' in line_stripped)):
                if steps:
                    steps[-1]['operations'].append(line_stripped)
        
        return steps
    
    def extract_pyspark_execution_steps(self, content):
        """Extract execution steps from PySpark script"""
        steps = []
        lines = content.split('\n')
        
        current_step = None
        for line in lines:
            line_stripped = line.strip()
            
            # Look for section headers
            if line_stripped.startswith('#') and ('STEP' in line_stripped or 'PHASE' in line_stripped or 'PASS' in line_stripped):
                current_step = line_stripped.replace('#', '').strip()
                steps.append({'step': current_step, 'operations': []})
            
            # Look for Spark operations
            elif current_step and ('.' in line_stripped and ('load' in line_stripped or 'select' in line_stripped or 'filter' in line_stripped or 'groupBy' in line_stripped or 'join' in line_stripped or 'save' in line_stripped)):
                if steps:
                    steps[-1]['operations'].append(line_stripped)
        
        return steps
    
    def extract_databricks_execution_steps(self, content):
        """Extract execution steps from Databricks notebook"""
        return self.extract_pyspark_execution_steps(content)  # Same as PySpark
    
    def extract_sql_execution_steps(self, content):
        """Extract execution steps from SQL script"""
        steps = []
        lines = content.split('\n')
        
        current_step = None
        for line in lines:
            line_stripped = line.strip()
            
            # Look for section headers
            if line_stripped.startswith('--') and ('STEP' in line_stripped or 'PHASE' in line_stripped):
                current_step = line_stripped.replace('--', '').strip()
                steps.append({'step': current_step, 'operations': []})
            
            # Look for SQL operations
            elif current_step and (line_stripped.upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP'))):
                if steps:
                    steps[-1]['operations'].append(line_stripped)
        
        return steps
    
    def extract_cm2_matching_passes(self, content):
        """Extract CM2 matching passes from Pig script"""
        passes = []
        lines = content.split('\n')
        
        for line in lines:
            line_stripped = line.strip()
            if 'PASS' in line_stripped and ('FIRST' in line_stripped or 'SECOND' in line_stripped or 'THIRD' in line_stripped or 'FOURTH' in line_stripped or 'FIFTH' in line_stripped or 'SIXTH' in line_stripped):
                passes.append(line_stripped)
        
        return passes
    
    def extract_pig_data_flow_detailed(self, content):
        """Extract detailed data flow from Pig script"""
        flow = []
        lines = content.split('\n')
        
        for line in lines:
            line_stripped = line.strip()
            if '=' in line_stripped and ('LOAD' in line_stripped or 'FOREACH' in line_stripped or 'FILTER' in line_stripped or 'GROUP' in line_stripped or 'JOIN' in line_stripped or 'STORE' in line_stripped):
                flow.append(line_stripped)
        
        return flow
    
    def extract_pyspark_data_flow_detailed(self, content):
        """Extract detailed data flow from PySpark script"""
        flow = []
        lines = content.split('\n')
        
        for line in lines:
            line_stripped = line.strip()
            if '=' in line_stripped and ('.' in line_stripped and ('load' in line_stripped or 'select' in line_stripped or 'filter' in line_stripped or 'groupBy' in line_stripped or 'join' in line_stripped or 'save' in line_stripped)):
                flow.append(line_stripped)
        
        return flow
    
    def extract_databricks_data_flow_detailed(self, content):
        """Extract detailed data flow from Databricks notebook"""
        return self.extract_pyspark_data_flow_detailed(content)  # Same as PySpark
    
    def extract_business_logic_from_comments(self, comments):
        """Extract business logic from comments"""
        logic_hints = []
        
        for comment in comments:
            comment_lower = comment.lower()
            
            for key, patterns in self.business_logic_patterns.items():
                for pattern in patterns:
                    if pattern in comment_lower:
                        logic_hints.append(f"{key}_processing")
                        break
        
        return list(set(logic_hints))
    
    def extract_python_functions_detailed(self, content):
        """Extract function definitions with their purposes"""
        functions = []
        
        # Extract function definitions
        pattern = r"def\s+(\w+)\s*\([^)]*\):"
        matches = re.findall(pattern, content)
        
        for func_name in matches:
            # Try to find docstring or comment for the function
            func_pattern = rf"def\s+{func_name}\s*\([^)]*\):\s*\n\s*\"\"\"([^\"]*)\"\"\""
            docstring_match = re.search(func_pattern, content, re.DOTALL)
            
            if docstring_match:
                docstring = docstring_match.group(1).strip()
                functions.append({'name': func_name, 'purpose': docstring})
            else:
                functions.append({'name': func_name, 'purpose': 'No documentation found'})
        
        return functions
    
    def build_hadoop_execution_flow(self):
        """Build Hadoop execution flow from analyzed processes"""
        # Group processes by workflow
        workflow_groups = defaultdict(list)
        
        for process in self.hadoop_processes:
            # Extract workflow name from path
            path_parts = process['path'].split('/')
            if 'app-' in '/'.join(path_parts):
                workflow_name = [part for part in path_parts if part.startswith('app-')][0]
                workflow_groups[workflow_name].append(process)
        
        # Build execution order
        for workflow_name, processes in workflow_groups.items():
            # Sort by file name to get execution order
            processes.sort(key=lambda x: x['name'])
            
            self.execution_flows[f"Hadoop_{workflow_name}"] = {
                'type': 'Hadoop Workflow',
                'processes': processes,
                'execution_order': [p['name'] for p in processes]
            }
    
    def build_databricks_execution_flow(self):
        """Build Databricks execution flow from analyzed processes"""
        # Group processes by folder
        folder_groups = defaultdict(list)
        
        for process in self.databricks_processes:
            # Extract folder name from path
            path_parts = process['path'].split('/')
            if len(path_parts) > 1:
                folder_name = path_parts[-2]  # Parent folder
                folder_groups[folder_name].append(process)
        
        # Build execution order
        for folder_name, processes in folder_groups.items():
            # Sort by file name to get execution order
            processes.sort(key=lambda x: x['name'])
            
            self.execution_flows[f"Databricks_{folder_name}"] = {
                'type': 'Databricks Workflow',
                'processes': processes,
                'execution_order': [p['name'] for p in processes]
            }
    
    def create_detailed_flow_excel(self, output_file="DETAILED_EXECUTION_FLOW.xlsx"):
        """Create detailed Excel file with execution flows"""
        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        
        # Create Execution Flow Overview sheet
        self.create_execution_flow_overview_sheet(wb)
        
        # Create Hadoop Detailed Flow sheet
        self.create_hadoop_detailed_flow_sheet(wb)
        
        # Create Databricks Detailed Flow sheet
        self.create_databricks_detailed_flow_sheet(wb)
        
        # Create Process Comparison sheet
        self.create_process_comparison_sheet(wb)
        
        wb.save(output_file)
        print(f"Detailed execution flow Excel file created: {output_file}")
    
    def create_execution_flow_overview_sheet(self, wb):
        """Create Execution Flow Overview sheet"""
        ws = wb.create_sheet("Execution_Flow_Overview")
        
        headers = [
            "Environment", "Workflow/Process", "Type", "Execution Order", "Key Processes", "Purpose"
        ]
        ws.append(headers)
        
        for flow_name, flow_data in self.execution_flows.items():
            if 'Hadoop' in flow_name:
                env = "Hadoop"
                processes = flow_data.get('processes', [])
                key_processes = [p['name'] for p in processes[:5]]  # First 5 processes
                purpose = self.infer_workflow_purpose(processes)
            else:
                env = "Databricks"
                processes = flow_data.get('processes', [])
                key_processes = [p['name'] for p in processes[:5]]  # First 5 processes
                purpose = self.infer_workflow_purpose(processes)
            
            row = [
                env,
                flow_name,
                flow_data.get('type', 'Unknown'),
                ' → '.join(flow_data.get('execution_order', [])),
                ', '.join(key_processes),
                purpose
            ]
            ws.append(row)
        
        self.format_sheet(ws)
    
    def create_hadoop_detailed_flow_sheet(self, wb):
        """Create Hadoop Detailed Flow sheet"""
        ws = wb.create_sheet("Hadoop_Detailed_Flow")
        
        headers = [
            "Process Name", "Type", "Path", "Execution Steps", "CM2 Passes", "Business Logic", "Comments Summary"
        ]
        ws.append(headers)
        
        for process in self.hadoop_processes:
            execution_steps = []
            for step in process.get('execution_steps', []):
                execution_steps.append(f"{step.get('step', 'Unknown')}: {len(step.get('operations', []))} operations")
            
            cm2_passes = process.get('cm2_passes', [])
            comments_summary = self.summarize_comments(process.get('comments', []))
            
            row = [
                process['name'],
                process['type'],
                process['relative_path'],
                '; '.join(execution_steps),
                '; '.join(cm2_passes),
                ', '.join(process['business_logic']),
                comments_summary
            ]
            ws.append(row)
        
        self.format_sheet(ws)
    
    def create_databricks_detailed_flow_sheet(self, wb):
        """Create Databricks Detailed Flow sheet"""
        ws = wb.create_sheet("Databricks_Detailed_Flow")
        
        headers = [
            "Process Name", "Type", "Path", "Execution Steps", "Functions", "Business Logic", "Comments Summary"
        ]
        ws.append(headers)
        
        for process in self.databricks_processes:
            execution_steps = []
            for step in process.get('execution_steps', []):
                execution_steps.append(f"{step.get('step', 'Unknown')}: {len(step.get('operations', []))} operations")
            
            functions = [f['name'] for f in process.get('functions', [])]
            comments_summary = self.summarize_comments(process.get('comments', []))
            
            row = [
                process['name'],
                process['type'],
                process['relative_path'],
                '; '.join(execution_steps),
                ', '.join(functions),
                ', '.join(process['business_logic']),
                comments_summary
            ]
            ws.append(row)
        
        self.format_sheet(ws)
    
    def create_process_comparison_sheet(self, wb):
        """Create Process Comparison sheet"""
        ws = wb.create_sheet("Process_Comparison")
        
        headers = [
            "Hadoop Process", "Hadoop Purpose", "Databricks Process", "Databricks Purpose", "Similarity", "Key Differences"
        ]
        ws.append(headers)
        
        # Create mappings between Hadoop and Databricks processes
        for hadoop_process in self.hadoop_processes:
            best_match = self.find_best_databricks_match(hadoop_process)
            
            if best_match:
                row = [
                    hadoop_process['name'],
                    self.infer_process_purpose(hadoop_process),
                    best_match['name'],
                    self.infer_process_purpose(best_match),
                    f"{best_match['similarity']:.2f}",
                    '; '.join(best_match['differences'])
                ]
                ws.append(row)
        
        self.format_sheet(ws)
    
    def infer_workflow_purpose(self, processes):
        """Infer workflow purpose from processes"""
        purposes = []
        for process in processes:
            purposes.extend(process.get('business_logic', []))
        
        # Count most common purposes
        purpose_counts = {}
        for purpose in purposes:
            purpose_counts[purpose] = purpose_counts.get(purpose, 0) + 1
        
        # Return top 3 purposes
        sorted_purposes = sorted(purpose_counts.items(), key=lambda x: x[1], reverse=True)
        return ', '.join([p[0] for p in sorted_purposes[:3]])
    
    def infer_process_purpose(self, process):
        """Infer process purpose from business logic and comments"""
        purposes = process.get('business_logic', [])
        comments = process.get('comments', [])
        
        # Look for purpose indicators in comments
        for comment in comments:
            comment_lower = comment.lower()
            if 'purpose' in comment_lower or 'function' in comment_lower or 'does' in comment_lower:
                return comment[:100] + "..." if len(comment) > 100 else comment
        
        return ', '.join(purposes) if purposes else 'Unknown'
    
    def summarize_comments(self, comments):
        """Summarize comments"""
        if not comments:
            return "No comments"
        
        # Take first few meaningful comments
        meaningful_comments = []
        for comment in comments[:3]:
            if len(comment.strip()) > 10:  # Skip very short comments
                meaningful_comments.append(comment[:50] + "..." if len(comment) > 50 else comment)
        
        return '; '.join(meaningful_comments) if meaningful_comments else "No meaningful comments"
    
    def find_best_databricks_match(self, hadoop_process):
        """Find best Databricks match for Hadoop process"""
        best_match = None
        best_score = 0
        
        for databricks_process in self.databricks_processes:
            score = self.calculate_process_similarity(hadoop_process, databricks_process)
            if score > best_score:
                best_score = score
                best_match = {
                    'name': databricks_process['name'],
                    'similarity': score,
                    'differences': self.get_process_differences(hadoop_process, databricks_process)
                }
        
        return best_match if best_score > 0.3 else None
    
    def calculate_process_similarity(self, hadoop_process, databricks_process):
        """Calculate similarity between processes"""
        score = 0.0
        
        # Filename similarity
        hadoop_name = hadoop_process['name'].lower()
        databricks_name = databricks_process['name'].lower()
        
        common_keywords = ['permid', 'policy', 'address', 'consumer', 'phone', 'merge', 'publish', 'validate', 'parse', 'cm2', 'bdf']
        for keyword in common_keywords:
            if keyword in hadoop_name and keyword in databricks_name:
                score += 0.2
        
        # Business logic similarity
        hadoop_logic = set(hadoop_process['business_logic'])
        databricks_logic = set(databricks_process['business_logic'])
        logic_intersection = hadoop_logic.intersection(databricks_logic)
        if hadoop_logic or databricks_logic:
            score += len(logic_intersection) / max(len(hadoop_logic), len(databricks_logic)) * 0.4
        
        # Comments similarity
        hadoop_comments = ' '.join(hadoop_process.get('comments', [])).lower()
        databricks_comments = ' '.join(databricks_process.get('comments', [])).lower()
        
        common_terms = ['permid', 'policy', 'address', 'consumer', 'phone', 'merge', 'publish', 'validate', 'parse']
        for term in common_terms:
            if term in hadoop_comments and term in databricks_comments:
                score += 0.05
        
        return min(score, 1.0)
    
    def get_process_differences(self, hadoop_process, databricks_process):
        """Get differences between processes"""
        differences = []
        
        # Type difference
        if hadoop_process['type'] != databricks_process['type']:
            differences.append(f"Type: {hadoop_process['type']} vs {databricks_process['type']}")
        
        # Business logic differences
        hadoop_logic = set(hadoop_process['business_logic'])
        databricks_logic = set(databricks_process['business_logic'])
        
        hadoop_only = hadoop_logic - databricks_logic
        databricks_only = databricks_logic - hadoop_logic
        
        if hadoop_only:
            differences.append(f"Hadoop only: {', '.join(hadoop_only)}")
        if databricks_only:
            differences.append(f"Databricks only: {', '.join(databricks_only)}")
        
        return differences
    
    def format_sheet(self, ws):
        """Format Excel sheet with headers and styling"""
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        header_alignment = Alignment(horizontal="center", vertical="center")
        
        # Format header row
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment
        
        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
        
        # Add borders
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for row in ws.iter_rows():
            for cell in row:
                cell.border = thin_border

def main():
    """Main function to run the enhanced flow analyzer"""
    analyzer = EnhancedFlowAnalyzer()
    
    print("=== Enhanced Execution Flow Analyzer ===")
    print("This tool analyzes execution flow, dependencies, and detailed logic in both Hadoop and Databricks.")
    print()
    
    hadoop_path = input("Enter Hadoop repository path: ").strip()
    databricks_path = input("Enter Azure Databricks repository path: ").strip()
    
    if not hadoop_path or not databricks_path:
        print("Error: Both repository paths are required!")
        return
    
    if not os.path.exists(hadoop_path):
        print(f"Error: Hadoop repository path does not exist: {hadoop_path}")
        return
    
    if not os.path.exists(databricks_path):
        print(f"Error: Databricks repository path does not exist: {databricks_path}")
        return
    
    # Analyze repositories
    analyzer.analyze_hadoop_execution_flow(hadoop_path)
    analyzer.analyze_databricks_execution_flow(databricks_path)
    
    # Create detailed Excel output
    output_file = input("Enter output Excel filename (default: DETAILED_EXECUTION_FLOW.xlsx): ").strip()
    if not output_file:
        output_file = "DETAILED_EXECUTION_FLOW.xlsx"
    
    analyzer.create_detailed_flow_excel(output_file)
    
    print(f"\nAnalysis complete!")
    print(f"Found {len(analyzer.hadoop_processes)} Hadoop processes")
    print(f"Found {len(analyzer.databricks_processes)} Databricks processes")
    print(f"Analyzed {len(analyzer.execution_flows)} execution flows")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    main()
