#!/usr/bin/env python3
"""
AI-Powered Source-to-Target Mapping Tool
========================================

This comprehensive tool uses Azure OpenAI to analyze Hadoop and Databricks repositories,
generating detailed source-to-target field mappings with intelligent comparison capabilities.

Key Features:
- Azure OpenAI integration for intelligent code analysis
- Deep Hadoop repository analysis (Oozie, PySpark, Pig, Hive)
- Databricks pipeline and notebook analysis
- Field-level source-to-target mapping
- PII detection and data type analysis
- Comprehensive comparison between Hadoop and Databricks
- Excel reports with exact column structure requested

Author: AI Assistant
Date: January 2025
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
import google.generativeai as genai
import base64
import hashlib

@dataclass
class FieldMapping:
    """Represents a field mapping with all required attributes"""
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
class TableAnalysis:
    """Represents analysis of a single table"""
    table_name: str
    schema_name: str
    technology: str  # hadoop/databricks
    repository: str
    field_mappings: List[FieldMapping]
    source_tables: List[str]
    target_tables: List[str]
    business_logic: str
    processing_order: int

@dataclass
class RepositoryAnalysis:
    """Complete repository analysis result"""
    repository_name: str
    repository_type: str  # hadoop/databricks
    tables: List[TableAnalysis]
    total_tables: int
    total_fields: int
    pii_fields: int
    primary_keys: int

class GeminiAnalyzer:
    """Google Gemini integration for intelligent code analysis"""
    
    def __init__(self, api_key: str, model: str = "gemini-1.5-pro"):
        """Initialize Gemini client"""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model)
    
    def analyze_code_for_field_mapping(self, code_content: str, file_type: str, table_context: str = "") -> Dict[str, Any]:
        """Use Gemini to analyze code and extract field mappings"""
        try:
            prompt = self._create_field_mapping_prompt(code_content, file_type, table_context)
            
            response = self.model.generate_content(prompt)
            return self._parse_ai_response(response.text)
            
        except Exception as e:
            print(f"⚠️ Gemini Analysis Error: {e}")
            return self._get_fallback_analysis(code_content, file_type)
    
    def _create_field_mapping_prompt(self, code_content: str, file_type: str, table_context: str) -> str:
        """Create AI prompt for field mapping analysis"""
        prompt = f"""
Analyze the following {file_type} code and extract detailed field mappings. Focus on:

1. Source tables and fields being read
2. Target tables and fields being written
3. Field transformations and mappings
4. Data types and field properties
5. Primary keys and PII indicators
6. Business logic and processing rules

Code:
```{file_type}
{code_content[:3000]}  # Limit content for token efficiency
```

Table Context: {table_context}

Please provide a JSON response with this structure:
{{
    "source_tables": ["table1", "table2"],
    "target_tables": ["target_table"],
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
            # Try to extract JSON from response
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
            "source_tables": self._extract_tables_from_text(response, "source"),
            "target_tables": self._extract_tables_from_text(response, "target"),
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
            r'\.read\.table\(["\'](\w+)["\']'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            tables.extend(matches)
        
        return list(set(tables))
    
    def _extract_field_mappings_from_text(self, text: str) -> List[Dict[str, Any]]:
        """Extract field mappings from text"""
        mappings = []
        # Simple pattern matching for field mappings
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
        # Look for processing-related keywords
        processing_keywords = ['transform', 'process', 'clean', 'validate', 'filter']
        rules = []
        
        for keyword in processing_keywords:
            if keyword in text.lower():
                rules.append(f"Contains {keyword} operations")
        
        return "; ".join(rules) if rules else "No specific processing rules identified"
    
    def _get_fallback_analysis(self, code_content: str, file_type: str) -> Dict[str, Any]:
        """Fallback analysis when AI fails"""
        return {
            "source_tables": self._extract_tables_regex(code_content),
            "target_tables": self._extract_tables_regex(code_content),
            "field_mappings": [],
            "processing_rules": f"Fallback analysis for {file_type}",
            "field_dependencies": ""
        }
    
    def _extract_tables_regex(self, content: str) -> List[str]:
        """Extract table names using regex patterns"""
        tables = []
        patterns = [
            r'FROM\s+(\w+)',
            r'INTO\s+(\w+)',
            r'JOIN\s+(\w+)',
            r'\.write\.table\(["\'](\w+)["\']',
            r'\.read\.table\(["\'](\w+)["\']'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.extend(matches)
        
        return list(set(tables))

class HadoopRepositoryAnalyzer:
    """Enhanced Hadoop repository analyzer with AI-powered field mapping"""
    
    def __init__(self, repo_path: str, ai_analyzer: GeminiAnalyzer):
        self.repo_path = Path(repo_path)
        self.ai_analyzer = ai_analyzer
        self.used_scripts = set()
        self.execution_order = {}
        self.field_id_counter = 1
    
    def analyze_hadoop_repository(self, repo_name: str = "app-cdd") -> RepositoryAnalysis:
        """Analyze Hadoop repository and generate field mappings"""
        print(f"🔍 Analyzing Hadoop Repository: {repo_name}")
        
        # Find all script files
        script_files = self._find_all_scripts()
        print(f"📋 Found {len(script_files)} script files")
        
        # Find Oozie workflows
        oozie_workflows = self._find_oozie_workflows()
        print(f"📋 Found {len(oozie_workflows)} Oozie workflows")
        
        # Analyze execution flow from Oozie
        execution_flow = self._analyze_execution_flow(oozie_workflows)
        
        # Identify used vs unused scripts
        used_scripts, unused_scripts = self._identify_used_scripts(script_files, execution_flow)
        
        # Analyze tables and generate field mappings
        tables = self._analyze_tables_with_ai(used_scripts, execution_flow)
        
        # Calculate statistics
        total_fields = sum(len(table.field_mappings) for table in tables)
        pii_fields = sum(len([f for f in table.field_mappings if f.contains_pii]) for table in tables)
        primary_keys = sum(len([f for f in table.field_mappings if f.pk]) for table in tables)
        
        return RepositoryAnalysis(
            repository_name=repo_name,
            repository_type="hadoop",
            tables=tables,
            total_tables=len(tables),
            total_fields=total_fields,
            pii_fields=pii_fields,
            primary_keys=primary_keys
        )
    
    def _find_all_scripts(self) -> List[Path]:
        """Find all script files in the repository"""
        script_extensions = ['.py', '.pig', '.sql', '.sh', '.scala']
        script_files = []
        
        for ext in script_extensions:
            files = list(self.repo_path.glob(f"**/*{ext}"))
            script_files.extend(files)
        
        return script_files
    
    def _find_oozie_workflows(self) -> List[Path]:
        """Find Oozie workflow files"""
        return list(self.repo_path.glob("**/*.xml"))
    
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
            'order': 0
        }
    
    def _identify_used_scripts(self, script_files: List[Path], execution_flow: Dict[str, Any]) -> Tuple[Set[Path], Set[Path]]:
        """Identify which scripts are used vs unused"""
        used_scripts = set()
        
        # Extract script paths from execution flow
        for workflow in execution_flow.get('workflows', []):
            for action in workflow.get('actions', []):
                script_path = action.get('script_path', '')
                if script_path:
                    script_file = self._find_script_file(script_path)
                    if script_file:
                        used_scripts.add(script_file)
        
        unused_scripts = set(script_files) - used_scripts
        return used_scripts, unused_scripts
    
    def _find_script_file(self, script_path: str) -> Optional[Path]:
        """Find the actual script file from path"""
        clean_path = script_path.replace('${appPath}', '')
        clean_path = clean_path.replace('${wf:actionData(\'get-datetime\')[\'date\']}', '*')
        
        patterns = [
            f"**/{clean_path}",
            f"**/{Path(clean_path).name}",
            f"**/*{Path(clean_path).stem}*"
        ]
        
        for pattern in patterns:
            matches = list(self.repo_path.glob(pattern))
            if matches:
                return matches[0]
        
        return None
    
    def _analyze_tables_with_ai(self, used_scripts: Set[Path], execution_flow: Dict[str, Any]) -> List[TableAnalysis]:
        """Analyze tables using AI to generate field mappings"""
        tables = []
        table_analysis_map = {}
        
        for script_file in used_scripts:
            try:
                # Determine technology
                technology = self._determine_technology(script_file)
                
                # Read script content
                content = script_file.read_text()
                
                # Use AI to analyze the script
                ai_analysis = self.ai_analyzer.analyze_code_for_field_mapping(
                    content, 
                    technology, 
                    f"Script: {script_file.name}"
                )
                
                # Process AI analysis results
                self._process_ai_analysis(ai_analysis, script_file, technology, table_analysis_map)
                
            except Exception as e:
                print(f"⚠️ Error analyzing {script_file}: {e}")
        
        # Convert map to list
        for table_name, analysis in table_analysis_map.items():
            tables.append(analysis)
        
        return tables
    
    def _process_ai_analysis(self, ai_analysis: Dict[str, Any], script_file: Path, technology: str, table_analysis_map: Dict[str, TableAnalysis]):
        """Process AI analysis results and build table analysis"""
        source_tables = ai_analysis.get('source_tables', [])
        target_tables = ai_analysis.get('target_tables', [])
        field_mappings = ai_analysis.get('field_mappings', [])
        
        # Process each target table
        for target_table in target_tables:
            if target_table not in table_analysis_map:
                table_analysis_map[target_table] = TableAnalysis(
                    table_name=target_table,
                    schema_name="default",
                    technology=technology,
                    repository="hadoop",
                    field_mappings=[],
                    source_tables=[],
                    target_tables=[target_table],
                    business_logic=f"Generated from {script_file.name}",
                    processing_order=0
                )
            
            # Add source tables
            table_analysis_map[target_table].source_tables.extend(source_tables)
            
            # Process field mappings
            for mapping in field_mappings:
                field_mapping = FieldMapping(
                    id=str(self.field_id_counter),
                    partner="CDD",
                    schema="default",
                    target_table_name=target_table,
                    target_field_name=mapping.get('target_field', 'unknown'),
                    target_field_data_type=mapping.get('data_type', 'string'),
                    pk=mapping.get('is_pk', False),
                    contains_pii=mapping.get('contains_pii', False),
                    field_type="transformed",
                    field_depends_on=mapping.get('field_dependencies', ''),
                    processing_order=self.field_id_counter,
                    pre_processing_rules=mapping.get('transformation', ''),
                    source_field_names=mapping.get('source_field', ''),
                    source_dataset_name=', '.join(source_tables),
                    field_definition=mapping.get('business_logic', ''),
                    example_1="",
                    example_2=""
                )
                
                table_analysis_map[target_table].field_mappings.append(field_mapping)
                self.field_id_counter += 1
    
    def _determine_technology(self, script_file: Path) -> str:
        """Determine technology based on file extension"""
        ext = script_file.suffix.lower()
        if ext == '.py':
            return 'pyspark'
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

class DatabricksRepositoryAnalyzer:
    """Databricks repository analyzer with AI-powered field mapping"""
    
    def __init__(self, repo_path: str, ai_analyzer: GeminiAnalyzer):
        self.repo_path = Path(repo_path)
        self.ai_analyzer = ai_analyzer
        self.field_id_counter = 1
    
    def analyze_databricks_repository(self, repo_name: str = "CDD") -> RepositoryAnalysis:
        """Analyze Databricks repository and generate field mappings"""
        print(f"🔍 Analyzing Databricks Repository: {repo_name}")
        
        # Find all notebook files
        notebook_files = self._find_notebook_files()
        print(f"📋 Found {len(notebook_files)} notebook files")
        
        # Find pipeline definitions
        pipeline_files = self._find_pipeline_files()
        print(f"📋 Found {len(pipeline_files)} pipeline files")
        
        # Analyze notebooks and generate field mappings
        tables = self._analyze_notebooks_with_ai(notebook_files)
        
        # Calculate statistics
        total_fields = sum(len(table.field_mappings) for table in tables)
        pii_fields = sum(len([f for f in table.field_mappings if f.contains_pii]) for table in tables)
        primary_keys = sum(len([f for f in table.field_mappings if f.pk]) for table in tables)
        
        return RepositoryAnalysis(
            repository_name=repo_name,
            repository_type="databricks",
            tables=tables,
            total_tables=len(tables),
            total_fields=total_fields,
            pii_fields=pii_fields,
            primary_keys=primary_keys
        )
    
    def _find_notebook_files(self) -> List[Path]:
        """Find Databricks notebook files"""
        notebook_extensions = ['.py', '.sql', '.scala', '.r']
        notebooks = []
        
        for ext in notebook_extensions:
            files = list(self.repo_path.glob(f"**/*{ext}"))
            notebooks.extend(files)
        
        return notebooks
    
    def _find_pipeline_files(self) -> List[Path]:
        """Find Databricks pipeline definition files"""
        return list(self.repo_path.glob("**/*pipeline*.json"))
    
    def _analyze_notebooks_with_ai(self, notebook_files: List[Path]) -> List[TableAnalysis]:
        """Analyze notebooks using AI to generate field mappings"""
        tables = []
        table_analysis_map = {}
        
        for notebook_file in notebook_files:
            try:
                # Determine technology
                technology = self._determine_technology(notebook_file)
                
                # Read notebook content
                content = notebook_file.read_text()
                
                # Use AI to analyze the notebook
                ai_analysis = self.ai_analyzer.analyze_code_for_field_mapping(
                    content, 
                    technology, 
                    f"Notebook: {notebook_file.name}"
                )
                
                # Process AI analysis results
                self._process_ai_analysis(ai_analysis, notebook_file, technology, table_analysis_map)
                
            except Exception as e:
                print(f"⚠️ Error analyzing {notebook_file}: {e}")
        
        # Convert map to list
        for table_name, analysis in table_analysis_map.items():
            tables.append(analysis)
        
        return tables
    
    def _process_ai_analysis(self, ai_analysis: Dict[str, Any], notebook_file: Path, technology: str, table_analysis_map: Dict[str, TableAnalysis]):
        """Process AI analysis results and build table analysis"""
        source_tables = ai_analysis.get('source_tables', [])
        target_tables = ai_analysis.get('target_tables', [])
        field_mappings = ai_analysis.get('field_mappings', [])
        
        # Process each target table
        for target_table in target_tables:
            if target_table not in table_analysis_map:
                table_analysis_map[target_table] = TableAnalysis(
                    table_name=target_table,
                    schema_name="default",
                    technology=technology,
                    repository="databricks",
                    field_mappings=[],
                    source_tables=[],
                    target_tables=[target_table],
                    business_logic=f"Generated from {notebook_file.name}",
                    processing_order=0
                )
            
            # Add source tables
            table_analysis_map[target_table].source_tables.extend(source_tables)
            
            # Process field mappings
            for mapping in field_mappings:
                field_mapping = FieldMapping(
                    id=str(self.field_id_counter),
                    partner="CDD",
                    schema="default",
                    target_table_name=target_table,
                    target_field_name=mapping.get('target_field', 'unknown'),
                    target_field_data_type=mapping.get('data_type', 'string'),
                    pk=mapping.get('is_pk', False),
                    contains_pii=mapping.get('contains_pii', False),
                    field_type="transformed",
                    field_depends_on=mapping.get('field_dependencies', ''),
                    processing_order=self.field_id_counter,
                    pre_processing_rules=mapping.get('transformation', ''),
                    source_field_names=mapping.get('source_field', ''),
                    source_dataset_name=', '.join(source_tables),
                    field_definition=mapping.get('business_logic', ''),
                    example_1="",
                    example_2=""
                )
                
                table_analysis_map[target_table].field_mappings.append(field_mapping)
                self.field_id_counter += 1
    
    def _determine_technology(self, notebook_file: Path) -> str:
        """Determine technology based on file extension"""
        ext = notebook_file.suffix.lower()
        if ext == '.py':
            return 'pyspark'
        elif ext == '.sql':
            return 'sql'
        elif ext == '.scala':
            return 'spark'
        elif ext == '.r':
            return 'r'
        else:
            return 'unknown'

class ComparisonEngine:
    """Engine to compare Hadoop and Databricks implementations"""
    
    def __init__(self):
        self.comparison_results = []
    
    def compare_repositories(self, hadoop_analysis: RepositoryAnalysis, databricks_analysis: RepositoryAnalysis) -> Dict[str, Any]:
        """Compare Hadoop and Databricks repository analyses"""
        print("🔍 Comparing Hadoop vs Databricks Implementations...")
        
        comparison = {
            'hadoop_tables': hadoop_analysis.tables,
            'databricks_tables': databricks_analysis.tables,
            'table_comparisons': [],
            'field_comparisons': [],
            'differences': [],
            'missing_in_databricks': [],
            'missing_in_hadoop': [],
            'different_transformations': []
        }
        
        # Compare tables
        hadoop_table_names = {table.table_name for table in hadoop_analysis.tables}
        databricks_table_names = {table.table_name for table in databricks_analysis.tables}
        
        # Find common tables
        common_tables = hadoop_table_names & databricks_table_names
        
        # Find missing tables
        comparison['missing_in_databricks'] = list(hadoop_table_names - databricks_table_names)
        comparison['missing_in_hadoop'] = list(databricks_table_names - hadoop_table_names)
        
        # Compare common tables
        for table_name in common_tables:
            hadoop_table = next(t for t in hadoop_analysis.tables if t.table_name == table_name)
            databricks_table = next(t for t in databricks_analysis.tables if t.table_name == table_name)
            
            table_comparison = self._compare_tables(hadoop_table, databricks_table)
            comparison['table_comparisons'].append(table_comparison)
            
            # Find field differences
            field_differences = self._compare_fields(hadoop_table, databricks_table)
            comparison['field_comparisons'].extend(field_differences)
        
        return comparison
    
    def _compare_tables(self, hadoop_table: TableAnalysis, databricks_table: TableAnalysis) -> Dict[str, Any]:
        """Compare two tables"""
        return {
            'table_name': hadoop_table.table_name,
            'hadoop_fields': len(hadoop_table.field_mappings),
            'databricks_fields': len(databricks_table.field_mappings),
            'field_difference': len(hadoop_table.field_mappings) - len(databricks_table.field_mappings),
            'hadoop_source_tables': hadoop_table.source_tables,
            'databricks_source_tables': databricks_table.source_tables,
            'source_table_difference': set(hadoop_table.source_tables) - set(databricks_table.source_tables)
        }
    
    def _compare_fields(self, hadoop_table: TableAnalysis, databricks_table: TableAnalysis) -> List[Dict[str, Any]]:
        """Compare fields between two tables"""
        differences = []
        
        hadoop_fields = {f.target_field_name for f in hadoop_table.field_mappings}
        databricks_fields = {f.target_field_name for f in databricks_table.field_mappings}
        
        # Find missing fields
        missing_in_databricks = hadoop_fields - databricks_fields
        missing_in_hadoop = databricks_fields - hadoop_fields
        
        for field_name in missing_in_databricks:
            differences.append({
                'table_name': hadoop_table.table_name,
                'field_name': field_name,
                'difference_type': 'missing_in_databricks',
                'hadoop_field': 'exists',
                'databricks_field': 'missing'
            })
        
        for field_name in missing_in_hadoop:
            differences.append({
                'table_name': hadoop_table.table_name,
                'field_name': field_name,
                'difference_type': 'missing_in_hadoop',
                'hadoop_field': 'missing',
                'databricks_field': 'exists'
            })
        
        return differences

class ExcelReportGenerator:
    """Generate comprehensive Excel reports with source-to-target mappings"""
    
    def __init__(self):
        self.report_data = {}
    
    def generate_hadoop_report(self, analysis: RepositoryAnalysis, output_file: str = "HADOOP_SOURCE_TARGET_MAPPING.xlsx"):
        """Generate Hadoop source-to-target mapping Excel report"""
        print(f"📊 Generating Hadoop Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Create one sheet per table
            for table in analysis.tables:
                sheet_name = table.table_name[:31]  # Excel sheet name limit
                
                # Prepare data for this table
                table_data = []
                for field in table.field_mappings:
                    table_data.append({
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
                
                # Write to Excel sheet
                df = pd.DataFrame(table_data)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Create summary sheet
            summary_data = []
            for table in analysis.tables:
                summary_data.append({
                    'Table Name': table.table_name,
                    'Schema': table.schema_name,
                    'Technology': table.technology,
                    'Total Fields': len(table.field_mappings),
                    'Primary Keys': len([f for f in table.field_mappings if f.pk]),
                    'PII Fields': len([f for f in table.field_mappings if f.contains_pii]),
                    'Source Tables': ', '.join(table.source_tables),
                    'Business Logic': table.business_logic
                })
            
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
        
        print(f"✅ Hadoop Excel report generated: {output_file}")
    
    def generate_databricks_report(self, analysis: RepositoryAnalysis, output_file: str = "DATABRICKS_SOURCE_TARGET_MAPPING.xlsx"):
        """Generate Databricks source-to-target mapping Excel report"""
        print(f"📊 Generating Databricks Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Create one sheet per table
            for table in analysis.tables:
                sheet_name = table.table_name[:31]  # Excel sheet name limit
                
                # Prepare data for this table
                table_data = []
                for field in table.field_mappings:
                    table_data.append({
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
                
                # Write to Excel sheet
                df = pd.DataFrame(table_data)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Create summary sheet
            summary_data = []
            for table in analysis.tables:
                summary_data.append({
                    'Table Name': table.table_name,
                    'Schema': table.schema_name,
                    'Technology': table.technology,
                    'Total Fields': len(table.field_mappings),
                    'Primary Keys': len([f for f in table.field_mappings if f.pk]),
                    'PII Fields': len([f for f in table.field_mappings if f.contains_pii]),
                    'Source Tables': ', '.join(table.source_tables),
                    'Business Logic': table.business_logic
                })
            
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
        
        print(f"✅ Databricks Excel report generated: {output_file}")
    
    def generate_comparison_report(self, comparison: Dict[str, Any], output_file: str = "HADOOP_DATABRICKS_COMPARISON.xlsx"):
        """Generate comparison report between Hadoop and Databricks"""
        print(f"📊 Generating Comparison Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Table Comparison Sheet
            table_comparison_data = []
            for table_comp in comparison['table_comparisons']:
                table_comparison_data.append({
                    'Table Name': table_comp['table_name'],
                    'Hadoop Fields': table_comp['hadoop_fields'],
                    'Databricks Fields': table_comp['databricks_fields'],
                    'Field Difference': table_comp['field_difference'],
                    'Hadoop Source Tables': ', '.join(table_comp['hadoop_source_tables']),
                    'Databricks Source Tables': ', '.join(table_comp['databricks_source_tables']),
                    'Source Table Differences': ', '.join(table_comp['source_table_difference'])
                })
            
            df_table_comp = pd.DataFrame(table_comparison_data)
            df_table_comp.to_excel(writer, sheet_name='Table Comparison', index=False)
            
            # Field Differences Sheet
            field_differences_data = []
            for field_diff in comparison['field_comparisons']:
                field_differences_data.append({
                    'Table Name': field_diff['table_name'],
                    'Field Name': field_diff['field_name'],
                    'Difference Type': field_diff['difference_type'],
                    'Hadoop Field': field_diff['hadoop_field'],
                    'Databricks Field': field_diff['databricks_field']
                })
            
            df_field_diff = pd.DataFrame(field_differences_data)
            df_field_diff.to_excel(writer, sheet_name='Field Differences', index=False)
            
            # Missing Tables Sheet
            missing_data = []
            for table in comparison['missing_in_databricks']:
                missing_data.append({
                    'Table Name': table,
                    'Missing In': 'Databricks',
                    'Status': 'Present in Hadoop only'
                })
            
            for table in comparison['missing_in_hadoop']:
                missing_data.append({
                    'Table Name': table,
                    'Missing In': 'Hadoop',
                    'Status': 'Present in Databricks only'
                })
            
            df_missing = pd.DataFrame(missing_data)
            df_missing.to_excel(writer, sheet_name='Missing Tables', index=False)
        
        print(f"✅ Comparison Excel report generated: {output_file}")

class AIPoweredSourceTargetMapper:
    """Main class for AI-powered source-to-target mapping"""
    
    def __init__(self, gemini_api_key: str):
        """Initialize the mapper with Gemini credentials"""
        self.ai_analyzer = GeminiAnalyzer(gemini_api_key)
        self.comparison_engine = ComparisonEngine()
        self.excel_generator = ExcelReportGenerator()
    
    def analyze_repositories(self, hadoop_repo_path: str, databricks_repo_path: str, 
                          hadoop_repo_name: str = "app-cdd", databricks_repo_name: str = "CDD") -> Dict[str, Any]:
        """Analyze both Hadoop and Databricks repositories"""
        print("🚀 Starting AI-Powered Source-to-Target Mapping Analysis")
        print("=" * 70)
        
        # Analyze Hadoop repository
        hadoop_analyzer = HadoopRepositoryAnalyzer(hadoop_repo_path, self.ai_analyzer)
        hadoop_analysis = hadoop_analyzer.analyze_hadoop_repository(hadoop_repo_name)
        
        # Analyze Databricks repository
        databricks_analyzer = DatabricksRepositoryAnalyzer(databricks_repo_path, self.ai_analyzer)
        databricks_analysis = databricks_analyzer.analyze_databricks_repository(databricks_repo_name)
        
        # Compare implementations
        comparison = self.comparison_engine.compare_repositories(hadoop_analysis, databricks_analysis)
        
        return {
            'hadoop_analysis': hadoop_analysis,
            'databricks_analysis': databricks_analysis,
            'comparison': comparison
        }
    
    def generate_all_reports(self, analysis_results: Dict[str, Any], output_prefix: str = "AI_MAPPING"):
        """Generate all Excel reports"""
        print("📊 Generating All Excel Reports...")
        
        # Generate Hadoop report
        hadoop_file = f"{output_prefix}_HADOOP_SOURCE_TARGET_MAPPING.xlsx"
        self.excel_generator.generate_hadoop_report(analysis_results['hadoop_analysis'], hadoop_file)
        
        # Generate Databricks report
        databricks_file = f"{output_prefix}_DATABRICKS_SOURCE_TARGET_MAPPING.xlsx"
        self.excel_generator.generate_databricks_report(analysis_results['databricks_analysis'], databricks_file)
        
        # Generate comparison report
        comparison_file = f"{output_prefix}_COMPARISON_REPORT.xlsx"
        self.excel_generator.generate_comparison_report(analysis_results['comparison'], comparison_file)
        
        print("✅ All reports generated successfully!")
        return {
            'hadoop_report': hadoop_file,
            'databricks_report': databricks_file,
            'comparison_report': comparison_file
        }
    
    def print_analysis_summary(self, analysis_results: Dict[str, Any]):
        """Print comprehensive analysis summary"""
        hadoop_analysis = analysis_results['hadoop_analysis']
        databricks_analysis = analysis_results['databricks_analysis']
        comparison = analysis_results['comparison']
        
        print("\n📊 ANALYSIS SUMMARY")
        print("=" * 50)
        
        print(f"\n🔷 HADOOP REPOSITORY ({hadoop_analysis.repository_name})")
        print(f"   Total Tables: {hadoop_analysis.total_tables}")
        print(f"   Total Fields: {hadoop_analysis.total_fields}")
        print(f"   Primary Keys: {hadoop_analysis.primary_keys}")
        print(f"   PII Fields: {hadoop_analysis.pii_fields}")
        
        print(f"\n🔷 DATABRICKS REPOSITORY ({databricks_analysis.repository_name})")
        print(f"   Total Tables: {databricks_analysis.total_tables}")
        print(f"   Total Fields: {databricks_analysis.total_fields}")
        print(f"   Primary Keys: {databricks_analysis.primary_keys}")
        print(f"   PII Fields: {databricks_analysis.pii_fields}")
        
        print(f"\n🔷 COMPARISON RESULTS")
        print(f"   Common Tables: {len(comparison['table_comparisons'])}")
        print(f"   Missing in Databricks: {len(comparison['missing_in_databricks'])}")
        print(f"   Missing in Hadoop: {len(comparison['missing_in_hadoop'])}")
        print(f"   Field Differences: {len(comparison['field_comparisons'])}")
        
        if comparison['missing_in_databricks']:
            print(f"\n⚠️ Tables missing in Databricks:")
            for table in comparison['missing_in_databricks']:
                print(f"   - {table}")
        
        if comparison['missing_in_hadoop']:
            print(f"\n⚠️ Tables missing in Hadoop:")
            for table in comparison['missing_in_hadoop']:
                print(f"   - {table}")

def main():
    """Main function to run the AI-powered source-to-target mapper"""
    print("🚀 AI-Powered Source-to-Target Mapping Tool (Gemini)")
    print("=" * 60)
    
    # Configuration - Replace with your Gemini API key
    GEMINI_API_KEY = os.getenv("GOOGLE_API_KEY", "your-gemini-api-key-here")
    
    # Repository paths - Replace with your actual paths
    HADOOP_REPO_PATH = "./OneDrive_1_7-25-2025/Hadoop/app-data-ingestion"
    DATABRICKS_REPO_PATH = "./mock_databricks_cdd"  # Replace with actual Databricks repo path
    
    # Repository names
    HADOOP_REPO_NAME = "app-cdd"
    DATABRICKS_REPO_NAME = "CDD"
    
    try:
        # Initialize mapper
        mapper = AIPoweredSourceTargetMapper(GEMINI_API_KEY)
        
        # Analyze repositories
        analysis_results = mapper.analyze_repositories(
            HADOOP_REPO_PATH, 
            DATABRICKS_REPO_PATH,
            HADOOP_REPO_NAME,
            DATABRICKS_REPO_NAME
        )
        
        # Generate reports
        report_files = mapper.generate_all_reports(analysis_results)
        
        # Print summary
        mapper.print_analysis_summary(analysis_results)
        
        print(f"\n✅ Analysis Complete!")
        print(f"📊 Reports generated:")
        for report_type, file_path in report_files.items():
            print(f"   {report_type}: {file_path}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Make sure to:")
        print("   1. Set your Gemini API key: export GOOGLE_API_KEY='your-key'")
        print("   2. Provide correct repository paths")
        print("   3. Install required dependencies: pip install google-generativeai pandas openpyxl")

if __name__ == "__main__":
    main()
