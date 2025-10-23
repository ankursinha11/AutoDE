#!/usr/bin/env python3
"""
Hadoop Oozie Workflow Analyzer
==============================

Specialized tool for analyzing Hadoop Oozie workflows:
- Maps each action with input/output paths and parameters
- Tracks source and target tables for each workflow
- Creates STTM mappings for each table/dataset
- Analyzes Pig, Spark, and transformation scripts

Configuration:
- Gemini API Key: AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA
- Hadoop Path: ./app-cdd
"""

import os
import re
import json
import xml.etree.ElementTree as ET
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd

# Install required packages
def install_packages():
    """Install required packages"""
    print("📦 Installing required packages...")
    packages = ["google-generativeai>=0.3.0", "pandas>=1.5.0", "openpyxl>=3.0.0"]
    
    for package in packages:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package], 
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f"   ✅ {package.split('>=')[0]} installed")
        except:
            print(f"   ⚠️ {package.split('>=')[0]} installation failed")

# Install packages first
install_packages()

# Now import the packages
try:
    import google.generativeai as genai
except ImportError:
    print("❌ Failed to import google.generativeai. Please install manually: pip install google-generativeai")
    sys.exit(1)

@dataclass
class WorkflowAction:
    """Represents a single action in an Oozie workflow"""
    name: str
    action_type: str  # shell, pig, map-reduce, java, etc.
    script_path: str
    input_paths: List[str]
    output_paths: List[str]
    parameters: Dict[str, str]
    dependencies: List[str]
    processing_order: int
    error_action: str
    ok_action: str

@dataclass
class DataTable:
    """Represents a data table/dataset"""
    table_name: str
    table_type: str  # source, target, intermediate
    hdfs_path: str
    schema: str
    format: str  # parquet, avro, text, etc.
    fields: List[str]
    transformations: List[str]

@dataclass
class FieldMapping:
    """Source-to-Target field mapping"""
    id: str
    partner: str
    schema: str
    target_table_name: str
    target_field_name: str
    target_field_data_type: str
    pk: bool
    contains_pii: bool
    field_type: str
    field_depends_on: str
    processing_order: int
    pre_processing_rules: str
    source_field_names: str
    source_dataset_name: str
    field_definition: str
    example_1: str
    example_2: str

@dataclass
class WorkflowAnalysis:
    """Complete analysis of a single workflow"""
    workflow_name: str
    workflow_path: str
    actions: List[WorkflowAction]
    source_tables: List[DataTable]
    target_tables: List[DataTable]
    intermediate_tables: List[DataTable]
    field_mappings: List[FieldMapping]
    processing_order: List[str]
    business_logic: str

class GeminiAnalyzer:
    """Google Gemini integration for intelligent code analysis"""
    
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash"):
        """Initialize Gemini client"""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model)
    
    def analyze_transformation_script(self, script_content: str, script_type: str, context: str) -> Dict[str, Any]:
        """Use Gemini to analyze transformation scripts"""
        try:
            prompt = self._create_transformation_prompt(script_content, script_type, context)
            response = self.model.generate_content(prompt)
            return self._parse_ai_response(response.text)
        except Exception as e:
            print(f"⚠️ Gemini Analysis Error: {e}")
            return self._get_fallback_analysis(script_content, script_type)
    
    def _create_transformation_prompt(self, script_content: str, script_type: str, context: str) -> str:
        """Create AI prompt for transformation analysis"""
        prompt = f"""
Analyze the following {script_type} transformation script and extract detailed field mappings. Focus on:

1. Input data sources and their fields
2. Output data targets and their fields
3. Field transformations and mappings
4. Data types and field properties
5. Primary keys and PII indicators
6. Business logic and processing rules

IMPORTANT: Extract table names from HDFS paths. For example:
- /user/hadoop/CDD/output/es/scrubbed/ -> table name: "es_scrubbed"
- /data/warehouse/patientaccts/current/ -> table name: "patientaccts"
- hdfs://cluster/data/hospitals/ -> table name: "hospitals"

Context: {context}

Script:
```{script_type}
{script_content[:3000]}
```

Please provide a JSON response with this structure:
{{
    "input_tables": ["table1", "table2"],
    "output_tables": ["target_table"],
    "field_mappings": [
        {{
            "source_field": "source_field_name",
            "target_field": "target_field_name",
            "data_type": "string/int/date/etc",
            "is_pk": true/false,
            "contains_pii": true/false,
            "transformation": "description of transformation",
            "business_logic": "explanation of business purpose"
        }}
    ],
    "processing_rules": "description of any preprocessing rules",
    "field_dependencies": "description of field dependencies"
}}
"""
        return prompt
    
    def _parse_ai_response(self, response: str) -> Dict[str, Any]:
        """Parse AI response and extract structured data"""
        try:
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                return self._extract_unstructured_data(response)
        except:
            return self._extract_unstructured_data(response)
    
    def _extract_unstructured_data(self, response: str) -> Dict[str, Any]:
        """Extract data from unstructured AI response"""
        return {
            "input_tables": self._extract_tables_from_text(response, "input"),
            "output_tables": self._extract_tables_from_text(response, "output"),
            "field_mappings": self._extract_field_mappings_from_text(response),
            "processing_rules": self._extract_processing_rules(response),
            "field_dependencies": ""
        }
    
    def _extract_tables_from_text(self, text: str, table_type: str) -> List[str]:
        """Extract table names from text"""
        tables = []
        patterns = [
            r'(\w+)\s+(?:table|dataset)',
            r'FROM\s+(\w+)',
            r'INTO\s+(\w+)',
            r'\.write\.table\(["\'](\w+)["\']',
            r'\.read\.table\(["\'](\w+)["\']',
            r'/user/[^/]+/([^/\s]+)',
            r'/data/[^/]+/([^/\s]+)',
            r'hdfs://[^/]+/[^/]+/([^/\s]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            tables.extend(matches)
        
        return list(set(tables))
    
    def _extract_field_mappings_from_text(self, text: str) -> List[Dict[str, Any]]:
        """Extract field mappings from text"""
        mappings = []
        field_patterns = re.findall(r'(\w+)\s*->\s*(\w+)', text)
        for source, target in field_patterns:
            mappings.append({
                "source_field": source,
                "target_field": target,
                "data_type": "string",
                "is_pk": False,
                "contains_pii": False,
                "transformation": "direct mapping",
                "business_logic": "field mapping"
            })
        
        return mappings
    
    def _extract_processing_rules(self, text: str) -> str:
        """Extract processing rules from text"""
        processing_keywords = ['transform', 'process', 'clean', 'validate', 'filter']
        rules = []
        
        for keyword in processing_keywords:
            if keyword in text.lower():
                rules.append(f"Contains {keyword} operations")
        
        return "; ".join(rules) if rules else "No specific processing rules identified"
    
    def _get_fallback_analysis(self, script_content: str, script_type: str) -> Dict[str, Any]:
        """Fallback analysis when AI fails"""
        return {
            "input_tables": self._extract_tables_regex(script_content),
            "output_tables": self._extract_tables_regex(script_content),
            "field_mappings": [],
            "processing_rules": f"Fallback analysis for {script_type}",
            "field_dependencies": ""
        }
    
    def _extract_tables_regex(self, content: str) -> List[str]:
        """Extract table names using regex patterns - enhanced for HDFS paths"""
        tables = []
        
        # Enhanced patterns for HDFS paths and table references
        patterns = [
            # Standard SQL patterns
            r'FROM\s+(\w+)',
            r'INTO\s+(\w+)',
            r'JOIN\s+(\w+)',
            r'\.write\.table\(["\'](\w+)["\']',
            r'\.read\.table\(["\'](\w+)["\']',
            
            # HDFS path patterns
            r'/user/[^/]+/([^/\s]+)',
            r'/data/[^/]+/([^/\s]+)',
            r'hdfs://[^/]+/[^/]+/([^/\s]+)',
            r'path=["\']([^"\']+)["\']',
            r'input.*?["\']([^"\']+)["\']',
            r'output.*?["\']([^"\']+)["\']',
            
            # Oozie workflow patterns
            r'mapred\.input\.dir.*?["\']([^"\']+)["\']',
            r'mapred\.output\.dir.*?["\']([^"\']+)["\']',
            
            # Pig patterns
            r'LOAD\s+[\'"]([^\'"]+)[\'"]',
            r'STORE\s+\w+\s+INTO\s+[\'"]([^\'"]+)[\'"]',
            
            # Spark patterns
            r'spark\.read\.parquet\(["\']([^"\']+)["\']',
            r'spark\.read\.format\(["\']([^"\']+)["\']',
            r'\.write\.parquet\(["\']([^"\']+)["\']',
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.parquet\(["\']([^"\']+)["\']'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    # Handle patterns that return tuples
                    for item in match:
                        if item:
                            table_name = self._extract_table_name_from_path(item)
                            if table_name:
                                tables.append(table_name)
                else:
                    table_name = self._extract_table_name_from_path(match)
                    if table_name:
                        tables.append(table_name)
        
        return list(set(tables))
    
    def _extract_table_name_from_path(self, path: str) -> Optional[str]:
        """Extract meaningful table name from HDFS path or table reference"""
        if not path:
            return None
        
        # Clean up the path
        path = path.replace('hdfs://', '').replace('file://', '')
        path = path.replace('${wf:user()}', '').replace('${wf:user}', '')
        path = path.replace('${env:USER}', '').replace('${USER}', '')
        
        # Extract the last meaningful part
        parts = path.split('/')
        
        # Look for table-like patterns, prioritizing meaningful names
        for part in reversed(parts):
            if part and part not in ['current', 'data', 'publish', 'staging', 'input', 'output', 'temp', 'tmp', 'scratch']:
                # Clean up the table name
                table_name = re.sub(r'[^a-zA-Z0-9_]', '', part)
                if table_name and len(table_name) > 2:  # Avoid single letters
                    return table_name
        
        return None

class OozieWorkflowAnalyzer:
    """Analyzer for Oozie workflows"""
    
    def __init__(self, repo_path: str, ai_analyzer: GeminiAnalyzer):
        self.repo_path = Path(repo_path)
        self.ai_analyzer = ai_analyzer
        self.field_id_counter = 1
    
    def analyze_all_workflows(self) -> List[WorkflowAnalysis]:
        """Analyze all Oozie workflows in the repository"""
        print("🔍 Analyzing Oozie Workflows...")
        
        # Find all workflow.xml files
        workflow_files = list(self.repo_path.glob("**/workflow.xml"))
        print(f"📋 Found {len(workflow_files)} workflow files")
        
        workflows = []
        for workflow_file in workflow_files:
            try:
                workflow_analysis = self._analyze_single_workflow(workflow_file)
                workflows.append(workflow_analysis)
                print(f"✅ Analyzed workflow: {workflow_analysis.workflow_name}")
            except Exception as e:
                print(f"⚠️ Error analyzing {workflow_file}: {e}")
        
        return workflows
    
    def _analyze_single_workflow(self, workflow_file: Path) -> WorkflowAnalysis:
        """Analyze a single Oozie workflow"""
        workflow_name = workflow_file.parent.name
        print(f"🔍 Analyzing workflow: {workflow_name}")
        
        # Parse workflow XML
        tree = ET.parse(workflow_file)
        root = tree.getroot()
        
        # Extract actions
        actions = self._extract_workflow_actions(root, workflow_file.parent)
        
        # Analyze data flow
        source_tables, target_tables, intermediate_tables = self._analyze_data_flow(actions)
        
        # Generate field mappings
        field_mappings = self._generate_field_mappings(actions, source_tables, target_tables)
        
        # Determine processing order
        processing_order = self._determine_processing_order(actions)
        
        return WorkflowAnalysis(
            workflow_name=workflow_name,
            workflow_path=str(workflow_file),
            actions=actions,
            source_tables=source_tables,
            target_tables=target_tables,
            intermediate_tables=intermediate_tables,
            field_mappings=field_mappings,
            processing_order=processing_order,
            business_logic=f"Oozie workflow analysis for {workflow_name}"
        )
    
    def _extract_workflow_actions(self, root: ET.Element, workflow_dir: Path) -> List[WorkflowAction]:
        """Extract all actions from workflow XML"""
        actions = []
        action_order = 0
        
        for action_elem in root.findall('.//action'):
            action_name = action_elem.get('name', 'unknown')
            action_order += 1
            
            # Determine action type and extract details
            action_type, script_path, input_paths, output_paths, parameters = self._parse_action_details(action_elem, workflow_dir)
            
            # Get control flow
            ok_elem = action_elem.find('ok')
            error_elem = action_elem.find('error')
            ok_action = ok_elem.get('to', 'end') if ok_elem is not None else 'end'
            error_action = error_elem.get('to', 'fail') if error_elem is not None else 'fail'
            
            action = WorkflowAction(
                name=action_name,
                action_type=action_type,
                script_path=script_path,
                input_paths=input_paths,
                output_paths=output_paths,
                parameters=parameters,
                dependencies=[],
                processing_order=action_order,
                error_action=error_action,
                ok_action=ok_action
            )
            
            actions.append(action)
        
        return actions
    
    def _parse_action_details(self, action_elem: ET.Element, workflow_dir: Path) -> Tuple[str, str, List[str], List[str], Dict[str, str]]:
        """Parse action details based on action type"""
        action_type = "unknown"
        script_path = ""
        input_paths = []
        output_paths = []
        parameters = {}
        
        # Check for different action types
        if action_elem.find('.//shell') is not None:
            action_type = "shell"
            script_path, input_paths, output_paths, parameters = self._parse_shell_action(action_elem, workflow_dir)
        elif action_elem.find('.//pig') is not None:
            action_type = "pig"
            script_path, input_paths, output_paths, parameters = self._parse_pig_action(action_elem, workflow_dir)
        elif action_elem.find('.//map-reduce') is not None:
            action_type = "map-reduce"
            script_path, input_paths, output_paths, parameters = self._parse_mapreduce_action(action_elem, workflow_dir)
        elif action_elem.find('.//java') is not None:
            action_type = "java"
            script_path, input_paths, output_paths, parameters = self._parse_java_action(action_elem, workflow_dir)
        
        return action_type, script_path, input_paths, output_paths, parameters
    
    def _parse_shell_action(self, action_elem: ET.Element, workflow_dir: Path) -> Tuple[str, List[str], List[str], Dict[str, str]]:
        """Parse shell action details"""
        shell_elem = action_elem.find('.//shell')
        script_path = ""
        input_paths = []
        output_paths = []
        parameters = {}
        
        # Extract script path from exec and arguments
        exec_elem = shell_elem.find('exec')
        if exec_elem is not None:
            script_path = exec_elem.text or ""
            
            # Look for script arguments
            for arg_elem in shell_elem.findall('argument'):
                arg_text = arg_elem.text or ""
                if arg_text.endswith('.sh') or arg_text.endswith('.py'):
                    script_path = arg_text
                    break
        
        # Extract paths from script content if available
        if script_path:
            script_file = workflow_dir / script_path.split('/')[-1]
            if script_file.exists():
                script_content = script_file.read_text()
                input_paths = self._extract_paths_from_content(script_content, "input")
                output_paths = self._extract_paths_from_content(script_content, "output")
        
        return script_path, input_paths, output_paths, parameters
    
    def _parse_pig_action(self, action_elem: ET.Element, workflow_dir: Path) -> Tuple[str, List[str], List[str], Dict[str, str]]:
        """Parse Pig action details"""
        pig_elem = action_elem.find('.//pig')
        script_path = ""
        input_paths = []
        output_paths = []
        parameters = {}
        
        # Extract script path
        script_elem = pig_elem.find('script')
        if script_elem is not None:
            script_path = script_elem.text or ""
        
        # Extract paths from script content
        if script_path:
            script_file = workflow_dir / script_path.split('/')[-1]
            if script_file.exists():
                script_content = script_file.read_text()
                input_paths = self._extract_pig_paths(script_content, "LOAD")
                output_paths = self._extract_pig_paths(script_content, "STORE")
        
        return script_path, input_paths, output_paths, parameters
    
    def _parse_mapreduce_action(self, action_elem: ET.Element, workflow_dir: Path) -> Tuple[str, List[str], List[str], Dict[str, str]]:
        """Parse MapReduce action details"""
        mr_elem = action_elem.find('.//map-reduce')
        script_path = ""
        input_paths = []
        output_paths = []
        parameters = {}
        
        # Extract configuration properties
        config_elem = mr_elem.find('configuration')
        if config_elem is not None:
            for prop_elem in config_elem.findall('property'):
                name_elem = prop_elem.find('name')
                value_elem = prop_elem.find('value')
                if name_elem is not None and value_elem is not None:
                    prop_name = name_elem.text or ""
                    prop_value = value_elem.text or ""
                    parameters[prop_name] = prop_value
                    
                    # Extract input/output paths
                    if 'input' in prop_name.lower():
                        input_paths.append(prop_value)
                    elif 'output' in prop_name.lower():
                        output_paths.append(prop_value)
        
        return script_path, input_paths, output_paths, parameters
    
    def _parse_java_action(self, action_elem: ET.Element, workflow_dir: Path) -> Tuple[str, List[str], List[str], Dict[str, str]]:
        """Parse Java action details"""
        java_elem = action_elem.find('.//java')
        script_path = ""
        input_paths = []
        output_paths = []
        parameters = {}
        
        # Extract main class
        main_class_elem = java_elem.find('main-class')
        if main_class_elem is not None:
            script_path = main_class_elem.text or ""
        
        return script_path, input_paths, output_paths, parameters
    
    def _extract_paths_from_content(self, content: str, path_type: str) -> List[str]:
        """Extract paths from script content"""
        paths = []
        patterns = [
            rf'{path_type}.*?["\']([^"\']+)["\']',
            rf'{path_type}.*?=([^\s]+)',
            rf'hdfs://[^/]+/[^/]+/([^/\s]+)',
            rf'/user/[^/]+/([^/\s]+)',
            rf'/data/[^/]+/([^/\s]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            paths.extend(matches)
        
        return list(set(paths))
    
    def _extract_pig_paths(self, content: str, operation: str) -> List[str]:
        """Extract paths from Pig script"""
        paths = []
        if operation == "LOAD":
            pattern = r'LOAD\s+[\'"]([^\'"]+)[\'"]'
        else:  # STORE
            pattern = r'STORE\s+\w+\s+INTO\s+[\'"]([^\'"]+)[\'"]'
        
        matches = re.findall(pattern, content, re.IGNORECASE)
        paths.extend(matches)
        
        return list(set(paths))
    
    def _analyze_data_flow(self, actions: List[WorkflowAction]) -> Tuple[List[DataTable], List[DataTable], List[DataTable]]:
        """Analyze data flow through the workflow"""
        source_tables = []
        target_tables = []
        intermediate_tables = []
        
        all_paths = []
        for action in actions:
            all_paths.extend(action.input_paths)
            all_paths.extend(action.output_paths)
        
        # Categorize tables based on usage patterns
        for path in set(all_paths):
            table_name = self.ai_analyzer._extract_table_name_from_path(path)
            if table_name:
                # Determine table type based on path patterns
                if any(keyword in path.lower() for keyword in ['input', 'source', 'raw']):
                    table_type = "source"
                    source_tables.append(DataTable(
                        table_name=table_name,
                        table_type=table_type,
                        hdfs_path=path,
                        schema="default",
                        format="unknown",
                        fields=[],
                        transformations=[]
                    ))
                elif any(keyword in path.lower() for keyword in ['output', 'target', 'final']):
                    table_type = "target"
                    target_tables.append(DataTable(
                        table_name=table_name,
                        table_type=table_type,
                        hdfs_path=path,
                        schema="default",
                        format="unknown",
                        fields=[],
                        transformations=[]
                    ))
                else:
                    table_type = "intermediate"
                    intermediate_tables.append(DataTable(
                        table_name=table_name,
                        table_type=table_type,
                        hdfs_path=path,
                        schema="default",
                        format="unknown",
                        fields=[],
                        transformations=[]
                    ))
        
        return source_tables, target_tables, intermediate_tables
    
    def _generate_field_mappings(self, actions: List[WorkflowAction], source_tables: List[DataTable], target_tables: List[DataTable]) -> List[FieldMapping]:
        """Generate field mappings by analyzing transformation scripts"""
        field_mappings = []
        
        for action in actions:
            if action.script_path and action.action_type in ['pig', 'shell']:
                # Find and analyze the script
                script_file = self._find_script_file(action.script_path)
                if script_file and script_file.exists():
                    try:
                        script_content = script_file.read_text()
                        ai_analysis = self.ai_analyzer.analyze_transformation_script(
                            script_content, 
                            action.action_type, 
                            f"Action: {action.name}"
                        )
                        
                        # Convert AI analysis to field mappings
                        for mapping in ai_analysis.get('field_mappings', []):
                            field_mapping = FieldMapping(
                                id=str(self.field_id_counter),
                                partner="CDD",
                                schema="default",
                                target_table_name=mapping.get('target_field', 'unknown'),
                                target_field_name=mapping.get('target_field', 'unknown'),
                                target_field_data_type=mapping.get('data_type', 'string'),
                                pk=mapping.get('is_pk', False),
                                contains_pii=mapping.get('contains_pii', False),
                                field_type="transformed",
                                field_depends_on=mapping.get('field_dependencies', ''),
                                processing_order=self.field_id_counter,
                                pre_processing_rules=mapping.get('transformation', ''),
                                source_field_names=mapping.get('source_field', ''),
                                source_dataset_name=', '.join(ai_analysis.get('input_tables', [])),
                                field_definition=mapping.get('business_logic', ''),
                                example_1="",
                                example_2=""
                            )
                            
                            field_mappings.append(field_mapping)
                            self.field_id_counter += 1
                            
                    except Exception as e:
                        print(f"⚠️ Error analyzing script {action.script_path}: {e}")
        
        return field_mappings
    
    def _find_script_file(self, script_path: str) -> Optional[Path]:
        """Find script file in the repository"""
        # Try different possible locations
        possible_paths = [
            self.repo_path / script_path,
            self.repo_path / script_path.split('/')[-1],
            self.repo_path / "pig" / script_path.split('/')[-1],
            self.repo_path / "pyspark" / script_path.split('/')[-1],
            self.repo_path / "shell" / script_path.split('/')[-1],
            self.repo_path / "spark" / script_path.split('/')[-1]
        ]
        
        for path in possible_paths:
            if path.exists():
                return path
        
        return None
    
    def _determine_processing_order(self, actions: List[WorkflowAction]) -> List[str]:
        """Determine the processing order of actions"""
        # Sort by processing order
        sorted_actions = sorted(actions, key=lambda x: x.processing_order)
        return [action.name for action in sorted_actions]

class ExcelReportGenerator:
    """Generate comprehensive Excel reports for Oozie workflow analysis"""
    
    def generate_workflow_analysis_report(self, workflows: List[WorkflowAnalysis], output_file: str = "HADOOP_OOZIE_WORKFLOW_ANALYSIS.xlsx"):
        """Generate comprehensive workflow analysis Excel report"""
        print(f"📊 Generating Oozie Workflow Analysis Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Workflow Summary Sheet
            self._create_workflow_summary_sheet(writer, workflows)
            
            # Action Details Sheet
            self._create_action_details_sheet(writer, workflows)
            
            # Data Flow Sheet
            self._create_data_flow_sheet(writer, workflows)
            
            # Field Mappings Sheet (one per workflow)
            for workflow in workflows:
                self._create_field_mappings_sheet(writer, workflow)
        
        print(f"✅ Oozie Workflow Analysis Excel report generated: {output_file}")
    
    def _create_workflow_summary_sheet(self, writer: pd.ExcelWriter, workflows: List[WorkflowAnalysis]):
        """Create workflow summary sheet"""
        summary_data = []
        for workflow in workflows:
            summary_data.append({
                'Workflow Name': workflow.workflow_name,
                'Total Actions': len(workflow.actions),
                'Source Tables': len(workflow.source_tables),
                'Target Tables': len(workflow.target_tables),
                'Intermediate Tables': len(workflow.intermediate_tables),
                'Field Mappings': len(workflow.field_mappings),
                'Processing Order': ' -> '.join(workflow.processing_order),
                'Business Logic': workflow.business_logic
            })
        
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name='Workflow Summary', index=False)
    
    def _create_action_details_sheet(self, writer: pd.ExcelWriter, workflows: List[WorkflowAnalysis]):
        """Create action details sheet"""
        action_data = []
        for workflow in workflows:
            for action in workflow.actions:
                action_data.append({
                    'Workflow Name': workflow.workflow_name,
                    'Action Name': action.name,
                    'Action Type': action.action_type,
                    'Script Path': action.script_path,
                    'Input Paths': ', '.join(action.input_paths),
                    'Output Paths': ', '.join(action.output_paths),
                    'Processing Order': action.processing_order,
                    'OK Action': action.ok_action,
                    'Error Action': action.error_action,
                    'Parameters': json.dumps(action.parameters)
                })
        
        df_actions = pd.DataFrame(action_data)
        df_actions.to_excel(writer, sheet_name='Action Details', index=False)
    
    def _create_data_flow_sheet(self, writer: pd.ExcelWriter, workflows: List[WorkflowAnalysis]):
        """Create data flow sheet"""
        data_flow_data = []
        for workflow in workflows:
            # Source tables
            for table in workflow.source_tables:
                data_flow_data.append({
                    'Workflow Name': workflow.workflow_name,
                    'Table Name': table.table_name,
                    'Table Type': table.table_type,
                    'HDFS Path': table.hdfs_path,
                    'Schema': table.schema,
                    'Format': table.format
                })
            
            # Target tables
            for table in workflow.target_tables:
                data_flow_data.append({
                    'Workflow Name': workflow.workflow_name,
                    'Table Name': table.table_name,
                    'Table Type': table.table_type,
                    'HDFS Path': table.hdfs_path,
                    'Schema': table.schema,
                    'Format': table.format
                })
            
            # Intermediate tables
            for table in workflow.intermediate_tables:
                data_flow_data.append({
                    'Workflow Name': workflow.workflow_name,
                    'Table Name': table.table_name,
                    'Table Type': table.table_type,
                    'HDFS Path': table.hdfs_path,
                    'Schema': table.schema,
                    'Format': table.format
                })
        
        df_data_flow = pd.DataFrame(data_flow_data)
        df_data_flow.to_excel(writer, sheet_name='Data Flow', index=False)
    
    def _create_field_mappings_sheet(self, writer: pd.ExcelWriter, workflow: WorkflowAnalysis):
        """Create field mappings sheet for a workflow"""
        if not workflow.field_mappings:
            return
        
        field_data = []
        for field in workflow.field_mappings:
            field_data.append({
                'id': field.id,
                'Partner': field.partner,
                'Schema': field.schema,
                'Target Table Name': field.target_table_name,
                'Target Field Name': field.target_field_name,
                'Target Field Data Type': field.target_field_data_type,
                'pk?': field.pk,
                'contains_pii': field.contains_pii,
                'Field Type': field.field_type,
                'Field Depends On': field.field_depends_on,
                'Processing Order': field.processing_order,
                'Pre Processing Rules': field.pre_processing_rules,
                'Source Field Names': field.source_field_names,
                'Source Dataset Name': field.source_dataset_name,
                'Field Definition': field.field_definition,
                'Example 1': field.example_1,
                'Example 2': field.example_2
            })
        
        df_fields = pd.DataFrame(field_data)
        sheet_name = f"{workflow.workflow_name}_STTM"[:31]  # Excel sheet name limit
        df_fields.to_excel(writer, sheet_name=sheet_name, index=False)

def test_gemini_connection():
    """Test Gemini connection"""
    print("🔌 Testing Gemini Connection...")
    
    try:
        api_key = "AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA"
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        response = model.generate_content("Hello, this is a test.")
        print("✅ Gemini connection successful!")
        return True
    except Exception as e:
        print(f"❌ Gemini connection failed: {e}")
        return False

def main():
    """Main function - Hadoop Oozie Workflow Analysis"""
    print("🚀 Hadoop Oozie Workflow Analyzer")
    print("=" * 60)
    print("📋 Configuration:")
    print("   Gemini API Key: AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA")
    print("   Hadoop Path: ./app-cdd")
    print("=" * 60)
    
    # Test Gemini connection
    if not test_gemini_connection():
        print("❌ Gemini connection failed. Please check your internet connection.")
        return
    
    # Check Hadoop directory
    print("\n📁 Checking Hadoop repository directory...")
    if not os.path.exists("./app-cdd"):
        print("❌ Hadoop repository not found: ./app-cdd")
        print("   Please make sure the app-cdd directory exists")
        return
    
    print("✅ Hadoop repository directory found!")
    
    try:
        # Initialize components
        print("\n🔧 Initializing Hadoop Oozie Workflow Analyzer...")
        ai_analyzer = GeminiAnalyzer("AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA")
        workflow_analyzer = OozieWorkflowAnalyzer("./app-cdd", ai_analyzer)
        excel_generator = ExcelReportGenerator()
        
        # Analyze workflows
        print("\n📊 Analyzing Oozie Workflows...")
        workflows = workflow_analyzer.analyze_all_workflows()
        
        if not workflows:
            print("❌ No workflows found in the repository")
            return
        
        # Generate reports
        print("\n📋 Generating Excel Reports...")
        excel_generator.generate_workflow_analysis_report(workflows, "HADOOP_OOZIE_WORKFLOW_ANALYSIS.xlsx")
        
        # Print summary
        print("\n" + "="*60)
        print("📊 WORKFLOW ANALYSIS SUMMARY")
        print("="*60)
        
        total_actions = sum(len(w.actions) for w in workflows)
        total_source_tables = sum(len(w.source_tables) for w in workflows)
        total_target_tables = sum(len(w.target_tables) for w in workflows)
        total_field_mappings = sum(len(w.field_mappings) for w in workflows)
        
        print(f"\n🔷 WORKFLOW ANALYSIS RESULTS")
        print(f"   Total Workflows: {len(workflows)}")
        print(f"   Total Actions: {total_actions}")
        print(f"   Total Source Tables: {total_source_tables}")
        print(f"   Total Target Tables: {total_target_tables}")
        print(f"   Total Field Mappings: {total_field_mappings}")
        
        print(f"\n🔷 WORKFLOW DETAILS:")
        for workflow in workflows:
            print(f"   📋 {workflow.workflow_name}")
            print(f"      Actions: {len(workflow.actions)}")
            print(f"      Source Tables: {len(workflow.source_tables)}")
            print(f"      Target Tables: {len(workflow.target_tables)}")
            print(f"      Field Mappings: {len(workflow.field_mappings)}")
            print(f"      Processing Order: {' -> '.join(workflow.processing_order)}")
        
        print(f"\n✅ Analysis Complete!")
        print(f"📊 Report generated: HADOOP_OOZIE_WORKFLOW_ANALYSIS.xlsx")
        
        print(f"\n🎯 Report Contents:")
        print(f"   📄 Workflow Summary - Overview of all workflows")
        print(f"   📄 Action Details - Detailed action analysis")
        print(f"   📄 Data Flow - Source and target tables")
        print(f"   📄 [WorkflowName]_STTM - Field mappings for each workflow")
        
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        print(f"\n🔧 Troubleshooting Tips:")
        print(f"   1. Make sure you have internet connection for Gemini API")
        print(f"   2. Check that your Hadoop repository contains workflow.xml files")
        print(f"   3. Ensure you have the required packages installed")
        
        # Print detailed error information for debugging
        import traceback
        print(f"\n🔍 Detailed Error Information:")
        traceback.print_exc()

if __name__ == "__main__":
    main()
