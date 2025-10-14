#!/usr/bin/env python3
"""
Hadoop to Azure Databricks Migration Analyzer
=============================================

This tool analyzes Hadoop repositories and compares them with Azure Databricks
pipelines to create comprehensive migration mapping documents.

Key Features:
- Analyzes Hadoop CDD repo (Oozie, Pig, Hive, PySpark, Spark)
- Maps execution order from Oozie workflows
- Identifies unused scripts
- Analyzes Azure Databricks pipeline flow
- Compares logic between Hadoop and Databricks
- Generates final migration mapping
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
class HadoopScript:
    """Represents a Hadoop script"""
    name: str
    path: str
    technology: str  # spark, pig, hive, shell
    is_used: bool
    execution_order: int
    dependencies: List[str]
    input_tables: List[str]
    output_tables: List[str]
    business_logic: str

@dataclass
class DatabricksNotebook:
    """Represents a Databricks notebook"""
    name: str
    execution_order: int
    input_tables: List[str]
    output_tables: List[str]
    business_logic: str
    technology: str  # pyspark, sql, python

@dataclass
class MigrationMapping:
    """Represents mapping between Hadoop and Databricks"""
    hadoop_script: HadoopScript
    databricks_notebook: DatabricksNotebook
    migration_complexity: str  # low, medium, high
    logic_match: float  # 0.0 to 1.0
    notes: str

@dataclass
class CDDAnalysis:
    """Complete CDD analysis result"""
    hadoop_scripts: List[HadoopScript]
    databricks_notebooks: List[DatabricksNotebook]
    migration_mappings: List[MigrationMapping]
    unused_scripts: List[str]
    execution_flow: Dict[str, Any]

class HadoopCDDAnalyzer:
    """Analyzes Hadoop CDD repository"""
    
    def __init__(self, hadoop_repo_path: str):
        self.hadoop_repo_path = Path(hadoop_repo_path)
        self.used_scripts = set()
        self.execution_order = {}
        
    def analyze_hadoop_cdd(self) -> Dict[str, Any]:
        """Analyze Hadoop CDD repository"""
        print("🔍 Analyzing Hadoop CDD Repository...")
        
        # Find all script files
        script_files = self._find_all_scripts()
        print(f"📋 Found {len(script_files)} script files")
        
        # Find Oozie workflows
        oozie_workflows = self._find_oozie_workflows()
        print(f"📋 Found {len(oozie_workflows)} Oozie workflows")
        
        # Analyze execution order from Oozie
        execution_flow = self._analyze_execution_flow(oozie_workflows)
        
        # Identify used vs unused scripts
        used_scripts, unused_scripts = self._identify_used_scripts(script_files, execution_flow)
        
        # Analyze script details
        hadoop_scripts = self._analyze_script_details(used_scripts, execution_flow)
        
        return {
            'hadoop_scripts': hadoop_scripts,
            'unused_scripts': unused_scripts,
            'execution_flow': execution_flow,
            'total_scripts': len(script_files),
            'used_scripts_count': len(used_scripts),
            'unused_scripts_count': len(unused_scripts)
        }
    
    def _find_all_scripts(self) -> List[Path]:
        """Find all script files in the repository"""
        script_extensions = ['.py', '.pig', '.sql', '.sh', '.scala']
        script_files = []
        
        for ext in script_extensions:
            files = list(self.hadoop_repo_path.glob(f"**/*{ext}"))
            script_files.extend(files)
        
        return script_files
    
    def _find_oozie_workflows(self) -> List[Path]:
        """Find Oozie workflow files"""
        return list(self.hadoop_repo_path.glob("**/*.xml"))
    
    def _analyze_execution_flow(self, oozie_workflows: List[Path]) -> Dict[str, Any]:
        """Analyze execution flow from Oozie workflows"""
        execution_flow = {
            'workflows': [],
            'script_execution_order': [],
            'parallel_branches': [],
            'decision_points': []
        }
        
        for workflow_file in oozie_workflows:
            try:
                workflow_analysis = self._analyze_single_workflow(workflow_file)
                execution_flow['workflows'].append(workflow_analysis)
                
                # Extract script execution order
                for action in workflow_analysis.get('actions', []):
                    if action.get('script_path'):
                        execution_flow['script_execution_order'].append({
                            'script': action['script_path'],
                            'order': action.get('order', 0),
                            'workflow': workflow_file.name
                        })
                        
            except Exception as e:
                print(f"⚠️ Error analyzing {workflow_file}: {e}")
        
        return execution_flow
    
    def _analyze_single_workflow(self, workflow_file: Path) -> Dict[str, Any]:
        """Analyze a single Oozie workflow"""
        try:
            tree = ET.parse(workflow_file)
            root = tree.getroot()
            
            workflow_name = root.get('name', workflow_file.stem)
            actions = []
            
            # Extract actions
            for action in root.findall('.//{uri:oozie:workflow:0.5}action'):
                action_name = action.get('name')
                if not action_name:
                    continue
                
                action_info = self._parse_action(action, workflow_file)
                if action_info:
                    actions.append(action_info)
            
            return {
                'workflow_name': workflow_name,
                'workflow_file': str(workflow_file),
                'actions': actions
            }
            
        except Exception as e:
            print(f"⚠️ Error parsing {workflow_file}: {e}")
            return {}
    
    def _parse_action(self, action_elem, workflow_file: Path) -> Optional[Dict[str, Any]]:
        """Parse an individual Oozie action"""
        action_name = action_elem.get('name')
        
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
        
        if not action_type:
            return None
        
        return {
            'action_name': action_name,
            'action_type': action_type,
            'technology': technology,
            'script_path': script_path,
            'order': 0  # Will be filled by execution order analysis
        }
    
    def _identify_used_scripts(self, script_files: List[Path], execution_flow: Dict[str, Any]) -> Tuple[Set[Path], Set[Path]]:
        """Identify which scripts are used vs unused"""
        used_scripts = set()
        
        # Extract script paths from execution flow
        for workflow in execution_flow.get('workflows', []):
            for action in workflow.get('actions', []):
                script_path = action.get('script_path', '')
                if script_path:
                    # Try to find the actual script file
                    script_file = self._find_script_file(script_path)
                    if script_file:
                        used_scripts.add(script_file)
        
        # Find unused scripts
        unused_scripts = set(script_files) - used_scripts
        
        return used_scripts, unused_scripts
    
    def _find_script_file(self, script_path: str) -> Optional[Path]:
        """Find the actual script file from path"""
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
    
    def _analyze_script_details(self, used_scripts: Set[Path], execution_flow: Dict[str, Any]) -> List[HadoopScript]:
        """Analyze details of used scripts"""
        hadoop_scripts = []
        
        for script_file in used_scripts:
            try:
                # Determine technology
                technology = self._determine_technology(script_file)
                
                # Extract business logic
                business_logic = self._extract_business_logic(script_file)
                
                # Extract input/output tables
                input_tables, output_tables = self._extract_table_mappings(script_file)
                
                # Get execution order
                execution_order = self._get_execution_order(script_file, execution_flow)
                
                script = HadoopScript(
                    name=script_file.name,
                    path=str(script_file),
                    technology=technology,
                    is_used=True,
                    execution_order=execution_order,
                    dependencies=[],
                    input_tables=input_tables,
                    output_tables=output_tables,
                    business_logic=business_logic
                )
                
                hadoop_scripts.append(script)
                
            except Exception as e:
                print(f"⚠️ Error analyzing {script_file}: {e}")
        
        return hadoop_scripts
    
    def _determine_technology(self, script_file: Path) -> str:
        """Determine technology based on file extension"""
        ext = script_file.suffix.lower()
        if ext == '.py':
            return 'spark'  # Assume PySpark/Spark
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
    
    def _get_execution_order(self, script_file: Path, execution_flow: Dict[str, Any]) -> int:
        """Get execution order for script"""
        script_name = script_file.name
        
        for workflow in execution_flow.get('workflows', []):
            for i, action in enumerate(workflow.get('actions', [])):
                if script_name in action.get('script_path', ''):
                    return i + 1
        
        return 999  # Default high number for unused scripts

class AzureDatabricksCDDAnalyzer:
    """Analyzes Azure Databricks CDD pipeline"""
    
    def __init__(self, databricks_repo_path: str):
        self.databricks_repo_path = Path(databricks_repo_path)
        
    def analyze_databricks_cdd(self) -> Dict[str, Any]:
        """Analyze Azure Databricks CDD pipeline"""
        print("🔍 Analyzing Azure Databricks CDD Pipeline...")
        
        # Based on the pipeline diagram, analyze CDD flow
        cdd_flow = self._analyze_cdd_pipeline_flow()
        
        # Find Databricks notebooks
        notebooks = self._find_databricks_notebooks()
        
        # Map notebooks to pipeline steps
        notebook_mappings = self._map_notebooks_to_pipeline(notebooks, cdd_flow)
        
        return {
            'cdd_flow': cdd_flow,
            'notebooks': notebooks,
            'notebook_mappings': notebook_mappings
        }
    
    def _analyze_cdd_pipeline_flow(self) -> Dict[str, Any]:
        """Analyze CDD pipeline flow from the diagram"""
        # Based on the pipeline diagram description
        cdd_flow = {
            'pipeline_name': 'pl_dataingestion_abi_group1',
            'schedule': 'Everyday 1AM',
            'steps': [
                {
                    'step': 1,
                    'name': 'CDD es_prebdf',
                    'description': 'Pre-processing step for CDD data before BDF handling',
                    'technology': 'pyspark',
                    'execution_order': 1
                },
                {
                    'step': 2,
                    'name': 'CDD bdf_download',
                    'description': 'Downloads Batch Data Files (BDF) for CDD',
                    'technology': 'python',
                    'execution_order': 2
                },
                {
                    'step': 3,
                    'name': 'CDD es_postbdf',
                    'description': 'Post-processing step for CDD data after BDF handling',
                    'technology': 'pyspark',
                    'execution_order': 3
                },
                {
                    'step': 4,
                    'name': 'LR XREF es_postbdf',
                    'description': 'Lead Resolution Cross-Reference on processed CDD post-BDF data',
                    'technology': 'pyspark',
                    'execution_order': 4
                },
                {
                    'step': 5,
                    'name': 'LSB es_xref_lsb',
                    'description': 'Move cross-referenced data to Lead Staging Base for linking',
                    'technology': 'pyspark',
                    'execution_order': 5
                },
                {
                    'step': 6,
                    'name': 'LEAD PROP es_xref_lsb_propagation',
                    'description': 'Propagate leads derived from CDD LSB data',
                    'technology': 'pyspark',
                    'execution_order': 6
                }
            ]
        }
        
        return cdd_flow
    
    def _find_databricks_notebooks(self) -> List[Path]:
        """Find Databricks notebook files"""
        notebook_extensions = ['.py', '.sql', '.scala', '.r']
        notebooks = []
        
        for ext in notebook_extensions:
            files = list(self.databricks_repo_path.glob(f"**/*{ext}"))
            notebooks.extend(files)
        
        return notebooks
    
    def _map_notebooks_to_pipeline(self, notebooks: List[Path], cdd_flow: Dict[str, Any]) -> List[DatabricksNotebook]:
        """Map notebooks to pipeline steps"""
        databricks_notebooks = []
        
        for step in cdd_flow['steps']:
            # Try to find matching notebook
            matching_notebook = self._find_matching_notebook(notebooks, step)
            
            notebook = DatabricksNotebook(
                name=step['name'],
                execution_order=step['execution_order'],
                input_tables=[],
                output_tables=[],
                business_logic=step['description'],
                technology=step['technology']
            )
            
            databricks_notebooks.append(notebook)
        
        return databricks_notebooks
    
    def _find_matching_notebook(self, notebooks: List[Path], step: Dict[str, Any]) -> Optional[Path]:
        """Find notebook that matches pipeline step"""
        step_name = step['name'].lower()
        
        for notebook in notebooks:
            notebook_name = notebook.name.lower()
            if any(keyword in notebook_name for keyword in step_name.split()):
                return notebook
        
        return None

class MigrationComparator:
    """Compares Hadoop and Databricks implementations"""
    
    def __init__(self):
        self.comparison_results = []
    
    def compare_implementations(self, hadoop_analysis: Dict[str, Any], databricks_analysis: Dict[str, Any]) -> List[MigrationMapping]:
        """Compare Hadoop and Databricks implementations"""
        print("🔍 Comparing Hadoop vs Databricks Implementations...")
        
        migration_mappings = []
        
        hadoop_scripts = hadoop_analysis.get('hadoop_scripts', [])
        databricks_notebooks = databricks_analysis.get('notebooks', [])
        
        # Map Hadoop scripts to Databricks notebooks
        for hadoop_script in hadoop_scripts:
            matching_notebook = self._find_matching_notebook(hadoop_script, databricks_notebooks)
            
            if matching_notebook:
                # Compare logic
                logic_match = self._compare_logic(hadoop_script, matching_notebook)
                
                # Determine migration complexity
                complexity = self._determine_migration_complexity(hadoop_script, matching_notebook)
                
                mapping = MigrationMapping(
                    hadoop_script=hadoop_script,
                    databricks_notebook=matching_notebook,
                    migration_complexity=complexity,
                    logic_match=logic_match,
                    notes=self._generate_migration_notes(hadoop_script, matching_notebook)
                )
                
                migration_mappings.append(mapping)
        
        return migration_mappings
    
    def _find_matching_notebook(self, hadoop_script: HadoopScript, databricks_notebooks: List[DatabricksNotebook]) -> Optional[DatabricksNotebook]:
        """Find matching Databricks notebook for Hadoop script"""
        # Simple matching based on execution order and technology
        for notebook in databricks_notebooks:
            if notebook.execution_order == hadoop_script.execution_order:
                return notebook
        
        return None
    
    def _compare_logic(self, hadoop_script: HadoopScript, databricks_notebook: DatabricksNotebook) -> float:
        """Compare business logic between Hadoop and Databricks"""
        # Simple comparison based on business logic description
        hadoop_logic = hadoop_script.business_logic.lower()
        databricks_logic = databricks_notebook.business_logic.lower()
        
        # Calculate similarity
        common_words = set(hadoop_logic.split()) & set(databricks_logic.split())
        total_words = set(hadoop_logic.split()) | set(databricks_logic.split())
        
        if total_words:
            return len(common_words) / len(total_words)
        else:
            return 0.0
    
    def _determine_migration_complexity(self, hadoop_script: HadoopScript, databricks_notebook: DatabricksNotebook) -> str:
        """Determine migration complexity"""
        if hadoop_script.technology == 'spark' and databricks_notebook.technology == 'pyspark':
            return 'low'
        elif hadoop_script.technology == 'pig' and databricks_notebook.technology == 'pyspark':
            return 'high'
        elif hadoop_script.technology == 'hive' and databricks_notebook.technology == 'sql':
            return 'medium'
        elif hadoop_script.technology == 'shell' and databricks_notebook.technology == 'python':
            return 'medium'
        else:
            return 'high'
    
    def _generate_migration_notes(self, hadoop_script: HadoopScript, databricks_notebook: DatabricksNotebook) -> str:
        """Generate migration notes"""
        notes = []
        
        if hadoop_script.technology != databricks_notebook.technology:
            notes.append(f"Technology conversion: {hadoop_script.technology} → {databricks_notebook.technology}")
        
        if hadoop_script.input_tables != databricks_notebook.input_tables:
            notes.append("Input table mappings may need adjustment")
        
        if hadoop_script.output_tables != databricks_notebook.output_tables:
            notes.append("Output table mappings may need adjustment")
        
        return "; ".join(notes) if notes else "Direct migration possible"

class CDDMigrationAnalyzer:
    """Main class for CDD migration analysis"""
    
    def __init__(self, hadoop_repo_path: str, databricks_repo_path: str):
        self.hadoop_repo_path = hadoop_repo_path
        self.databricks_repo_path = databricks_repo_path
        
        self.hadoop_analyzer = HadoopCDDAnalyzer(hadoop_repo_path)
        self.databricks_analyzer = AzureDatabricksCDDAnalyzer(databricks_repo_path)
        self.comparator = MigrationComparator()
    
    def analyze_cdd_migration(self) -> CDDAnalysis:
        """Perform complete CDD migration analysis"""
        print("🚀 Starting CDD Migration Analysis")
        print("=" * 60)
        
        # Analyze Hadoop CDD
        hadoop_analysis = self.hadoop_analyzer.analyze_hadoop_cdd()
        
        # Analyze Azure Databricks CDD
        databricks_analysis = self.databricks_analyzer.analyze_databricks_cdd()
        
        # Compare implementations
        migration_mappings = self.comparator.compare_implementations(hadoop_analysis, databricks_analysis)
        
        # Create final analysis
        cdd_analysis = CDDAnalysis(
            hadoop_scripts=hadoop_analysis['hadoop_scripts'],
            databricks_notebooks=databricks_analysis['notebooks'],
            migration_mappings=migration_mappings,
            unused_scripts=hadoop_analysis['unused_scripts'],
            execution_flow=hadoop_analysis['execution_flow']
        )
        
        return cdd_analysis
    
    def generate_excel_report(self, analysis: CDDAnalysis, output_file: str = "cdd_migration_analysis.xlsx"):
        """Generate comprehensive Excel report"""
        print(f"📊 Generating Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Hadoop Scripts Analysis
            hadoop_data = []
            for script in analysis.hadoop_scripts:
                hadoop_data.append({
                    'Script Name': script.name,
                    'Path': script.path,
                    'Technology': script.technology,
                    'Execution Order': script.execution_order,
                    'Input Tables': ', '.join(script.input_tables),
                    'Output Tables': ', '.join(script.output_tables),
                    'Business Logic': script.business_logic,
                    'Is Used': script.is_used
                })
            
            pd.DataFrame(hadoop_data).to_excel(writer, sheet_name='Hadoop Scripts', index=False)
            
            # Databricks Notebooks Analysis
            databricks_data = []
            for notebook in analysis.databricks_notebooks:
                databricks_data.append({
                    'Notebook Name': notebook.name,
                    'Execution Order': notebook.execution_order,
                    'Technology': notebook.technology,
                    'Input Tables': ', '.join(notebook.input_tables),
                    'Output Tables': ', '.join(notebook.output_tables),
                    'Business Logic': notebook.business_logic
                })
            
            pd.DataFrame(databricks_data).to_excel(writer, sheet_name='Databricks Notebooks', index=False)
            
            # Migration Mappings
            migration_data = []
            for mapping in analysis.migration_mappings:
                migration_data.append({
                    'Hadoop Script': mapping.hadoop_script.name,
                    'Databricks Notebook': mapping.databricks_notebook.name,
                    'Migration Complexity': mapping.migration_complexity,
                    'Logic Match': f"{mapping.logic_match:.2%}",
                    'Notes': mapping.notes
                })
            
            pd.DataFrame(migration_data).to_excel(writer, sheet_name='Migration Mappings', index=False)
            
            # Unused Scripts
            unused_data = [{'Unused Script': str(script)} for script in analysis.unused_scripts]
            pd.DataFrame(unused_data).to_excel(writer, sheet_name='Unused Scripts', index=False)
            
            # Execution Flow
            execution_data = []
            for workflow in analysis.execution_flow.get('workflows', []):
                for action in workflow.get('actions', []):
                    execution_data.append({
                        'Workflow': workflow.get('workflow_name', ''),
                        'Action': action.get('action_name', ''),
                        'Technology': action.get('technology', ''),
                        'Script': action.get('script_path', '')
                    })
            
            pd.DataFrame(execution_data).to_excel(writer, sheet_name='Execution Flow', index=False)
        
        print(f"✅ Excel report generated: {output_file}")
    
    def generate_json_report(self, analysis: CDDAnalysis, output_file: str = "cdd_migration_analysis.json"):
        """Generate JSON report"""
        print(f"📄 Generating JSON report: {output_file}")
        
        # Convert analysis to serializable format
        serializable_analysis = {
            'hadoop_scripts': [asdict(script) for script in analysis.hadoop_scripts],
            'databricks_notebooks': [asdict(notebook) for notebook in analysis.databricks_notebooks],
            'migration_mappings': [asdict(mapping) for mapping in analysis.migration_mappings],
            'unused_scripts': [str(script) for script in analysis.unused_scripts],
            'execution_flow': analysis.execution_flow
        }
        
        with open(output_file, 'w') as f:
            json.dump(serializable_analysis, f, indent=2)
        
        print(f"✅ JSON report generated: {output_file}")

def main():
    """Main function"""
    print("🚀 Hadoop to Azure Databricks CDD Migration Analyzer")
    print("=" * 60)
    
    # Get paths from user
    hadoop_repo_path = input("Enter Hadoop CDD repository path: ").strip()
    databricks_repo_path = input("Enter Azure Databricks CDD repository path: ").strip()
    
    if not hadoop_repo_path or not databricks_repo_path:
        print("❌ Please provide both repository paths")
        return
    
    # Initialize analyzer
    analyzer = CDDMigrationAnalyzer(hadoop_repo_path, databricks_repo_path)
    
    # Perform analysis
    analysis = analyzer.analyze_cdd_migration()
    
    # Generate reports
    analyzer.generate_excel_report(analysis)
    analyzer.generate_json_report(analysis)
    
    # Print summary
    print(f"\n📊 Analysis Complete!")
    print(f"   Hadoop Scripts: {len(analysis.hadoop_scripts)}")
    print(f"   Databricks Notebooks: {len(analysis.databricks_notebooks)}")
    print(f"   Migration Mappings: {len(analysis.migration_mappings)}")
    print(f"   Unused Scripts: {len(analysis.unused_scripts)}")
    
    print(f"\n✅ Reports generated:")
    print(f"   📊 Excel: cdd_migration_analysis.xlsx")
    print(f"   📄 JSON: cdd_migration_analysis.json")

if __name__ == "__main__":
    main()
