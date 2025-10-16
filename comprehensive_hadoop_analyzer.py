#!/usr/bin/env python3
"""
Comprehensive Hadoop Repository Analysis Tool - UPGRADED
=======================================================

This tool provides hierarchical mapping analysis of Hadoop repositories:
1. High-level business flow
2. Pipeline-level flow
3. Execution order flow
4. Technology-specific flow
5. Granular detail flow

NEW FEATURES:
- Multi-repository support (analyze multiple repos in one run)
- Script dependency analysis (detect indirect script usage)
- Enhanced script-to-script call detection

Output: Complete Excel report with all mapping levels
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
from collections import defaultdict
import sys

@dataclass
class BusinessDomain:
    """Represents a business domain"""
    name: str
    workflows: List[str]
    data_sources: List[str]
    data_targets: List[str]
    technologies: List[str]
    total_scripts: int
    description: str

@dataclass
class PipelineFlow:
    """Represents a complete pipeline flow"""
    domain: str
    coordinator_name: str
    workflow_name: str
    frequency: str
    execution_order: int
    total_actions: int
    technologies_used: Dict[str, int]
    data_sources: List[str]
    data_targets: List[str]
    business_purpose: str

@dataclass
class ExecutionStep:
    """Represents a single execution step"""
    workflow_name: str
    step_number: int
    action_name: str
    technology: str
    script_name: str
    execution_order: int
    input_tables: List[str]
    output_tables: List[str]
    business_purpose: str
    dependencies: List[str]
    is_critical: bool

@dataclass
class TechnologyFlow:
    """Represents technology-specific flow"""
    technology: str
    script_count: int
    input_sources: List[str]
    output_targets: List[str]
    transformation_types: List[str]
    complexity_score: int
    business_domains: List[str]

@dataclass
class DataFlowDetail:
    """Represents detailed data flow"""
    source_table: str
    source_column: str
    transformation_logic: str
    target_table: str
    target_column: str
    business_rule: str
    data_quality_rule: str
    script_name: str
    technology: str

class ComprehensiveHadoopAnalyzer:
    """Comprehensive Hadoop repository analyzer - UPGRADED"""
    
    def __init__(self, hadoop_repo_path: str):
        self.hadoop_repo_path = Path(hadoop_repo_path)
        self.business_domains = {}
        self.pipeline_flows = []
        self.execution_steps = []
        self.technology_flows = {}
        self.data_flow_details = []
        self.table_registry = {}
        self.used_scripts = set()
        self.unused_scripts = set()
        self.all_scripts = []
        self.script_dependencies = {}  # NEW: Track script-to-script dependencies
        
    def analyze_hadoop_repository(self) -> Dict[str, Any]:
        """Perform comprehensive Hadoop repository analysis"""
        print("🚀 Comprehensive Hadoop Repository Analysis")
        print("=" * 60)
        
        # Step 1: Parse DDL files
        print("🔍 Step 1: Parsing DDL files...")
        self._parse_ddl_files()
        
        # Step 2: Find and analyze Oozie workflows
        print("🔍 Step 2: Analyzing Oozie workflows...")
        oozie_workflows = self._find_oozie_workflows()
        workflow_analyses = self._analyze_workflows_comprehensive(oozie_workflows)
        
        # Step 3: Find all scripts
        print("🔍 Step 3: Finding all scripts...")
        self.all_scripts = self._find_all_scripts()
        
        # Step 4: Identify used vs unused scripts (ENHANCED)
        print("🔍 Step 4: Identifying used vs unused scripts...")
        self._identify_used_scripts_enhanced(self.all_scripts, workflow_analyses)
        
        # Step 5: Analyze business domains
        print("🔍 Step 5: Analyzing business domains...")
        self._analyze_business_domains(workflow_analyses)
        
        # Step 6: Create pipeline flows
        print("🔍 Step 6: Creating pipeline flows...")
        self._create_pipeline_flows(workflow_analyses)
        
        # Step 7: Create execution steps
        print("🔍 Step 7: Creating execution steps...")
        self._create_execution_steps(workflow_analyses)
        
        # Step 8: Analyze technology flows
        print("🔍 Step 8: Analyzing technology flows...")
        self._analyze_technology_flows()
        
        # Step 9: Create detailed data flows
        print("🔍 Step 9: Creating detailed data flows...")
        self._create_detailed_data_flows()
        
        # Step 10: Generate comprehensive report
        return self._generate_comprehensive_report()
    
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
        """Find all Oozie workflow files - ENHANCED for CDD structure"""
        patterns = [
            "**/coordinators/*.xml",
            "**/workflows/**/*.xml",
            "**/oozie/*.xml",
            "**/oozie/*/workflow.xml",  # CDD specific pattern
            "**/oozie/*/coordinator.xml"  # CDD specific pattern
        ]
        
        workflow_files = []
        for pattern in patterns:
            files = list(self.hadoop_repo_path.glob(pattern))
            workflow_files.extend(files)
        
        # Remove duplicates
        workflow_files = list(set(workflow_files))
        
        print(f"   📋 Found {len(workflow_files)} Oozie workflow files")
        return workflow_files
    
    def _analyze_workflows_comprehensive(self, oozie_workflows: List[Path]) -> List[Dict[str, Any]]:
        """Analyze all Oozie workflows comprehensively"""
        workflow_analyses = []
        
        # Extract coordinators and workflows
        coordinators = []
        workflows = []
        
        for workflow_file in oozie_workflows:
            if 'coordinator' in workflow_file.name.lower():
                coordinators.append(workflow_file)
            else:
                workflows.append(workflow_file)
        
        print(f"   📊 Found {len(coordinators)} coordinators and {len(workflows)} workflows")
        
        # Process coordinators first to get scheduling info
        coordinator_info = {}
        for coord_file in coordinators:
            try:
                tree = ET.parse(coord_file)
                root = tree.getroot()
                
                coord_name = root.get('name', coord_file.stem)
                frequency = root.get('frequency', 'Unknown')
                
                # Find workflow path
                workflow_elem = root.find('.//{uri:oozie:coordinator:0.2}workflow')
                if workflow_elem is not None:
                    app_path_elem = workflow_elem.find('.//{uri:oozie:coordinator:0.2}app-path')
                    if app_path_elem is not None:
                        workflow_path = app_path_elem.text
                        workflow_name = Path(workflow_path).name.replace('_workflow', '').replace('_coordinator', '')
                        coordinator_info[workflow_name] = {
                            'name': coord_name,
                            'frequency': frequency,
                            'file': coord_file
                        }
                        print(f"   📅 Coordinator '{coord_name}' schedules workflow '{workflow_name}' (freq: {frequency})")
            except Exception as e:
                print(f"   ⚠️ Error parsing coordinator {coord_file}: {e}")
        
        # Determine execution order
        execution_order = self._determine_execution_order(workflows, coordinators)
        print(f"   📋 Workflow execution order determined: {len(execution_order)} workflows")
        
        # Analyze workflows
        for i, workflow_file in enumerate(workflows):
            try:
                execution_order_num = execution_order.get(workflow_file, i + 1)
                analysis = self._analyze_single_workflow_comprehensive(workflow_file, execution_order_num, coordinator_info)
                if analysis:
                    workflow_analyses.append(analysis)
                    print(f"   ✅ Analyzed: {analysis['name']} (Order: {analysis['workflow_execution_order']})")
            except Exception as e:
                print(f"   ❌ Error analyzing {workflow_file}: {e}")
        
        return workflow_analyses
    
    def _determine_execution_order(self, workflows: List[Path], coordinators: List[Path]) -> Dict[Path, int]:
        """Determine execution order based on coordinators and naming patterns"""
        execution_order = {}
        
        # Extract frequency information from coordinators
        freq_info = {}
        for coord_file in coordinators:
            try:
                tree = ET.parse(coord_file)
                root = tree.getroot()
                frequency = root.get('frequency', 'Unknown')
                
                # Parse frequency to get hours
                hours_match = re.search(r'hours\((\d+)\)', frequency)
                if hours_match:
                    hours = int(hours_match.group(1))
                    freq_info[coord_file] = hours
                else:
                    freq_info[coord_file] = 999  # Default for unknown frequency
            except:
                freq_info[coord_file] = 999
        
        # Sort coordinators by frequency (lower hours = higher priority)
        sorted_coords = sorted(freq_info.items(), key=lambda x: x[1])
        
        # Assign execution order
        order = 1
        for coord_file, hours in sorted_coords:
            execution_order[coord_file] = order
            order += 1
        
        # Assign order to workflows based on their coordinators or naming patterns
        for i, workflow_file in enumerate(workflows):
            if workflow_file not in execution_order:
                # Try to match with coordinator
                workflow_name = workflow_file.stem.lower()
                matched = False
                
                for coord_file in coordinators:
                    coord_name = coord_file.stem.lower()
                    if any(keyword in workflow_name for keyword in coord_name.split('_')):
                        execution_order[workflow_file] = execution_order.get(coord_file, order)
                        matched = True
                        break
                
                if not matched:
                    execution_order[workflow_file] = order
                    order += 1
        
        return execution_order
    
    def _analyze_single_workflow_comprehensive(self, workflow_file: Path, execution_order: int, coordinator_info: Dict) -> Optional[Dict[str, Any]]:
        """Analyze a single Oozie workflow comprehensively"""
        try:
            tree = ET.parse(workflow_file)
            root = tree.getroot()
            
            workflow_name = root.get('name', workflow_file.stem)
            
            # Get coordinator info if available
            coordinator_name = workflow_name
            frequency = "Unknown"
            if workflow_name in coordinator_info:
                coordinator_name = coordinator_info[workflow_name]['name']
                frequency = coordinator_info[workflow_name]['frequency']
            
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
                    
                    # Collect input/output tables
                    input_tables.extend(action_info.get('input_tables', []))
                    output_tables.extend(action_info.get('output_tables', []))
                    
                    # Collect data sources and targets
                    data_sources.extend(action_info.get('data_sources', []))
                    data_targets.extend(action_info.get('data_targets', []))
            
            # Determine business domain
            business_domain = self._determine_business_domain(workflow_name, actions)
            
            # Determine critical path
            critical_path = self._determine_critical_path(actions)
            
            return {
                'name': workflow_name,
                'file_path': str(workflow_file),
                'workflow_execution_order': execution_order,
                'frequency': frequency,
                'total_actions': len(actions),
                'actions': actions,
                'technologies': technologies,
                'input_tables': list(set(input_tables)),
                'output_tables': list(set(output_tables)),
                'data_sources': list(set(data_sources)),
                'data_targets': list(set(data_targets)),
                'business_domain': business_domain,
                'critical_path': critical_path,
                'business_purpose': self._determine_business_purpose(workflow_name, actions)
            }
            
        except Exception as e:
            print(f"   ⚠️ Error parsing {workflow_file}: {e}")
            return None
    
    def _parse_action_comprehensive(self, action_elem, workflow_file: Path, workflow_name: str) -> Optional[Dict[str, Any]]:
        """Parse an individual Oozie action comprehensively"""
        action_name = action_elem.get('name')
        if not action_name:
            return None
        
        # Determine action type and script
        action_type = None
        script_path = ""
        technology = ""
        
        # Check for Spark actions (try multiple namespace patterns)
        spark_elem = (action_elem.find('.//spark') or 
                     action_elem.find('.//{uri:oozie:spark-action:0.1}spark') or
                     action_elem.find('.//{uri:oozie:workflow:0.5}spark'))
        if spark_elem is not None:
            action_type = "spark"
            technology = "spark"
            jar_elem = None
            # Try different ways to find jar element
            for child in spark_elem:
                if child.tag.endswith('jar'):
                    jar_elem = child
                    break
            
            if jar_elem is None:
                # Fallback to original method
                jar_elem = (spark_elem.find('{uri:oozie:spark-action:0.1}jar') or 
                           spark_elem.find('jar') or
                           spark_elem.find('{uri:oozie:workflow:0.5}jar'))
            if jar_elem is not None:
                script_path = jar_elem.text or ""
        
        # Check for Pig actions (try multiple namespace patterns)
        pig_elem = (action_elem.find('.//pig') or 
                   action_elem.find('.//{uri:oozie:pig-action:0.1}pig') or
                   action_elem.find('.//{uri:oozie:workflow:0.5}pig'))
        if pig_elem is not None:
            action_type = "pig"
            technology = "pig"
            script_elem = (pig_elem.find('script') or 
                          pig_elem.find('{uri:oozie:pig-action:0.1}script') or
                          pig_elem.find('{uri:oozie:workflow:0.5}script'))
            if script_elem is not None:
                script_path = script_elem.text or ""
        
        # Check for Hive actions (try both with and without namespace)
        hive_elem = action_elem.find('.//hive') or action_elem.find('.//{uri:oozie:hive-action:0.2}hive')
        if hive_elem is not None:
            action_type = "hive"
            technology = "hive"
            script_elem = hive_elem.find('script') or hive_elem.find('{uri:oozie:hive-action:0.2}script')
            if script_elem is not None:
                script_path = script_elem.text or ""
        
        # Check for Shell actions (try multiple namespace patterns)
        shell_elem = (action_elem.find('.//shell') or 
                     action_elem.find('.//{uri:oozie:shell-action:0.3}shell') or
                     action_elem.find('.//{uri:oozie:workflow:0.5}shell'))
        if shell_elem is not None:
            action_type = "shell"
            technology = "shell"
            exec_elem = None
            # Try different ways to find exec element
            for child in shell_elem:
                if child.tag.endswith('exec'):
                    exec_elem = child
                    break
            
            if exec_elem is None:
                # Fallback to original method
                exec_elem = (shell_elem.find('{uri:oozie:shell-action:0.3}exec') or
                            shell_elem.find('exec') or
                            shell_elem.find('{uri:oozie:workflow:0.5}exec'))
            if exec_elem is not None:
                script_path = exec_elem.text or ""
        
        # Check for Email actions (try both with and without namespace)
        email_elem = action_elem.find('.//email') or action_elem.find('.//{uri:oozie:email-action:0.2}email')
        if email_elem is not None:
            action_type = "email"
            technology = "notification"
            script_path = "email_notification"
        
        if not action_type:
            return None
        
        # Extract comprehensive information from script
        script_info = self._analyze_script_comprehensive(script_path, workflow_file, technology)
        
        return {
            'action_name': action_name,
            'action_type': action_type,
            'technology': technology,
            'script_path': script_path,
            'script_name': self._extract_script_name(script_path),
            'input_tables': script_info.get('input_tables', []),
            'output_tables': script_info.get('output_tables', []),
            'data_sources': script_info.get('data_sources', []),
            'data_targets': script_info.get('data_targets', []),
            'business_purpose': script_info.get('business_purpose', 'Data Processing'),
            'transformations': script_info.get('transformations', []),
            'joins': script_info.get('joins', []),
            'data_quality_rules': script_info.get('data_quality_rules', []),
            'business_rules': script_info.get('business_rules', []),
            'column_mappings': script_info.get('column_mappings', []),
            'complexity_score': script_info.get('complexity_score', 0),
            'workflow_name': workflow_name
        }
    
    def _analyze_script_comprehensive(self, script_path: str, workflow_file: Path, technology: str) -> Dict[str, Any]:
        """Analyze script comprehensively"""
        script_info = {
            'input_tables': [],
            'output_tables': [],
            'data_sources': [],
            'data_targets': [],
            'business_purpose': 'Data Processing',
            'transformations': [],
            'joins': [],
            'data_quality_rules': [],
            'business_rules': [],
            'column_mappings': [],
            'complexity_score': 0
        }
        
        if not script_path or script_path == "email_notification":
            return script_info
        
        # Try to find the actual script file
        script_file = self._find_script_file(script_path, workflow_file)
        if not script_file or not script_file.exists():
            return script_info
        
        try:
            content = script_file.read_text()
            
            # Extract information based on technology
            if technology == 'spark':
                script_info.update(self._analyze_spark_script_comprehensive(content))
            elif technology == 'pig':
                script_info.update(self._analyze_pig_script_comprehensive(content))
            elif technology == 'hive':
                script_info.update(self._analyze_hive_script_comprehensive(content))
            elif technology == 'shell':
                script_info.update(self._analyze_shell_script_comprehensive(content))
                
        except Exception as e:
            print(f"   ⚠️ Error reading script {script_file}: {e}")
        
        return script_info
    
    def _analyze_spark_script_comprehensive(self, content: str) -> Dict[str, Any]:
        """Comprehensive analysis of Spark scripts"""
        info = {
            'input_tables': [],
            'output_tables': [],
            'data_sources': [],
            'data_targets': [],
            'transformations': [],
            'joins': [],
            'data_quality_rules': [],
            'business_rules': [],
            'column_mappings': [],
            'complexity_score': 0
        }
        
        # Extract input tables - improved patterns
        input_patterns = [
            r'spark\.read\.parquet\(["\']([^"\']+)["\']',
            r'spark\.read\.table\(["\']([^"\']+)["\']',
            r'spark\.read\.format\(["\']([^"\']+)["\']',
            r'\.read\.parquet\(["\']([^"\']+)["\']',
            r'\.read\.table\(["\']([^"\']+)["\']',
            r'\.read\.csv\(["\']([^"\']+)["\']',
            r'\.read\.json\(["\']([^"\']+)["\']',
            r'\.load\(["\']([^"\']+)["\']',
            r'\.option\(["\']path["\'],\s*["\']([^"\']+)["\']',
            r'\.option\(["\']dbtable["\'],\s*["\']([^"\']+)["\']',
            r'\.option\(["\']query["\'],\s*["\']([^"\']+)["\']'
        ]
        
        for pattern in input_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                table_name = self._extract_table_name_from_path(match)
                if table_name and table_name not in info['input_tables']:
                    info['input_tables'].append(table_name)
                    info['data_sources'].append(match)
        
        # Extract output tables - improved patterns
        output_patterns = [
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.parquet\(["\']([^"\']+)["\']',
            r'\.write\.parquet\(["\']([^"\']+)["\']',
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.table\(["\']([^"\']+)["\']',
            r'\.write\.table\(["\']([^"\']+)["\']',
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.csv\(["\']([^"\']+)["\']',
            r'\.write\.csv\(["\']([^"\']+)["\']',
            r'\.write\.mode\(["\']([^"\']+)["\']\)\.json\(["\']([^"\']+)["\']',
            r'\.write\.json\(["\']([^"\']+)["\']',
            r'\.saveAsTable\(["\']([^"\']+)["\']',
            r'\.save\(["\']([^"\']+)["\']'
        ]
        
        for pattern in output_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    path = match[1] if len(match) > 1 else match[0]
                else:
                    path = match
                table_name = self._extract_table_name_from_path(path)
                if table_name and table_name not in info['output_tables']:
                    info['output_tables'].append(table_name)
                    info['data_targets'].append(path)
        
        # Extract transformations
        transform_patterns = [
            (r'\.select\(([^)]+)\)', 'select'),
            (r'\.filter\(([^)]+)\)', 'filter'),
            (r'\.groupBy\(([^)]+)\)', 'groupBy'),
            (r'\.agg\(([^)]+)\)', 'aggregate'),
            (r'\.withColumn\(["\']([^"\']+)["\'],\s*([^)]+)\)', 'withColumn'),
            (r'\.drop\(["\']([^"\']+)["\']\)', 'drop'),
            (r'\.distinct\(\)', 'distinct'),
            (r'\.repartition\(([^)]+)\)', 'repartition')
        ]
        
        for pattern, transform_type in transform_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                info['transformations'].append({
                    'type': transform_type,
                    'expression': match if isinstance(match, str) else str(match),
                    'description': f'SPARK {transform_type}: {match}'
                })
        
        # Extract joins
        join_patterns = [
            r'\.join\(([^,]+),\s*([^,]+),\s*["\']([^"\']+)["\']\)',
            r'\.join\(([^,]+),\s*([^)]+)\)',
            r'\.leftJoin\(([^,]+),\s*([^)]+)\)',
            r'\.rightJoin\(([^,]+),\s*([^)]+)\)'
        ]
        
        for pattern in join_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                info['joins'].append({
                    'left_table': match[0].strip(),
                    'right_table': match[1].strip(),
                    'join_type': 'inner',
                    'join_condition': match[1] if len(match) >= 2 else '',
                    'description': f"Join {match[0]} with {match[1]}"
                })
        
        # Extract data quality rules
        dq_patterns = [
            (r'\.isNull\(\)', 'null_check'),
            (r'\.isNotNull\(\)', 'not_null_check'),
            (r'\.isNaN\(\)', 'nan_check'),
            (r'\.isIn\(([^)]+)\)', 'value_in_list'),
            (r'\.between\(([^,]+),\s*([^)]+)\)', 'range_check'),
            (r'\.contains\(["\']([^"\']+)["\']\)', 'contains_check')
        ]
        
        for pattern, rule_type in dq_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                info['data_quality_rules'].append({
                    'type': rule_type,
                    'condition': match if isinstance(match, str) else str(match),
                    'description': f"Data quality rule: {rule_type}"
                })
        
        # Extract business rules from comments
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
                info['business_rules'].append(match.strip())
        
        # Extract column mappings
        mapping_patterns = [
            (r'\.withColumn\(["\']([^"\']+)["\'],\s*([^)]+)\)', 'column_transform'),
            (r'\.select\(([^)]+)\)', 'column_select'),
            (r'\.alias\(["\']([^"\']+)["\']\)', 'column_alias')
        ]
        
        for pattern, mapping_type in mapping_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                info['column_mappings'].append({
                    'type': mapping_type,
                    'source_column': match[0] if isinstance(match, tuple) else '',
                    'target_column': match[0] if isinstance(match, str) else match[0],
                    'transformation': match[1] if isinstance(match, tuple) and len(match) > 1 else '',
                    'description': f"{mapping_type}: {match}"
                })
        
        # Calculate complexity score
        # Determine business purpose based on script content and patterns
        business_purpose = self._determine_script_business_purpose(content)
        info['business_purpose'] = business_purpose
        
        info['complexity_score'] = self._calculate_complexity(content)
        
        return info
    
    def _determine_script_business_purpose(self, content: str) -> str:
        """Determine business purpose from script content"""
        content_lower = content.lower()
        
        # Business purpose patterns
        if 'notification' in content_lower or 'email' in content_lower:
            return 'Notification Processing'
        elif 'audit' in content_lower or 'log' in content_lower:
            return 'Audit and Logging'
        elif 'parse' in content_lower or 'publish' in content_lower:
            return 'Data Parsing and Publishing'
        elif 'generate' in content_lower or 'create' in content_lower:
            return 'Data Generation'
        elif 'append' in content_lower or 'insert' in content_lower:
            return 'Data Appending'
        elif 'check' in content_lower or 'validate' in content_lower:
            return 'Data Validation'
        elif 'transform' in content_lower or 'convert' in content_lower:
            return 'Data Transformation'
        elif 'extract' in content_lower or 'load' in content_lower:
            return 'Data Extraction and Loading'
        elif 'reconcile' in content_lower or 'merge' in content_lower:
            return 'Data Reconciliation'
        elif 'ingest' in content_lower or 'ingestion' in content_lower:
            return 'Data Ingestion'
        else:
            return 'Data Processing'
    
    def _analyze_pig_script_comprehensive(self, content: str) -> Dict[str, Any]:
        """Comprehensive analysis of Pig scripts"""
        info = {
            'input_tables': [],
            'output_tables': [],
            'data_sources': [],
            'data_targets': [],
            'transformations': [],
            'joins': [],
            'data_quality_rules': [],
            'business_rules': [],
            'column_mappings': [],
            'complexity_score': 0
        }
        
        # Extract input tables
        input_pattern = r'LOAD\s+[\'"]([^\'"]+)[\'"]'
        matches = re.findall(input_pattern, content, re.IGNORECASE)
        for match in matches:
            table_name = self._extract_table_name_from_path(match)
            if table_name and table_name not in info['input_tables']:
                info['input_tables'].append(table_name)
                info['data_sources'].append(match)
        
        # Extract output tables
        output_pattern = r'STORE\s+\w+\s+INTO\s+[\'"]([^\'"]+)[\'"]'
        matches = re.findall(output_pattern, content, re.IGNORECASE)
        for match in matches:
            table_name = self._extract_table_name_from_path(match)
            if table_name and table_name not in info['output_tables']:
                info['output_tables'].append(table_name)
                info['data_targets'].append(match)
        
        # Extract transformations
        transform_patterns = [
            (r'FOREACH\s+([^;]+)', 'foreach'),
            (r'FILTER\s+([^;]+)', 'filter'),
            (r'GROUP\s+([^;]+)', 'group'),
            (r'ORDER\s+([^;]+)', 'order'),
            (r'DISTINCT\s+([^;]+)', 'distinct')
        ]
        
        for pattern, transform_type in transform_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                info['transformations'].append({
                    'type': transform_type,
                    'expression': match,
                    'description': f'PIG {transform_type}: {match}'
                })
        
        info['complexity_score'] = self._calculate_complexity(content)
        
        return info
    
    def _analyze_hive_script_comprehensive(self, content: str) -> Dict[str, Any]:
        """Comprehensive analysis of Hive scripts"""
        info = {
            'input_tables': [],
            'output_tables': [],
            'data_sources': [],
            'data_targets': [],
            'transformations': [],
            'joins': [],
            'data_quality_rules': [],
            'business_rules': [],
            'column_mappings': [],
            'complexity_score': 0
        }
        
        # Extract input tables
        input_patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'LEFT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'RIGHT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'INNER\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        ]
        
        for pattern in input_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match not in info['input_tables']:
                    info['input_tables'].append(match)
        
        # Extract output tables
        output_patterns = [
            r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'CREATE\s+EXTERNAL\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        ]
        
        for pattern in output_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if match not in info['output_tables']:
                    info['output_tables'].append(match)
        
        # Extract transformations
        transform_patterns = [
            (r'SELECT\s+([^FROM]+)', 'select'),
            (r'WHERE\s+([^GROUP|ORDER|HAVING]+)', 'filter'),
            (r'GROUP\s+BY\s+([^HAVING|ORDER]+)', 'groupBy'),
            (r'ORDER\s+BY\s+([^LIMIT]+)', 'orderBy'),
            (r'HAVING\s+([^ORDER|LIMIT]+)', 'having')
        ]
        
        for pattern, transform_type in transform_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                info['transformations'].append({
                    'type': transform_type,
                    'expression': match.strip(),
                    'description': f'HIVE {transform_type}: {match.strip()}'
                })
        
        # Extract joins
        join_patterns = [
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+ON\s+([^WHERE|GROUP|ORDER]+)',
            r'LEFT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+ON\s+([^WHERE|GROUP|ORDER]+)',
            r'RIGHT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+ON\s+([^WHERE|GROUP|ORDER]+)'
        ]
        
        for pattern in join_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                info['joins'].append({
                    'left_table': '',
                    'right_table': match[0],
                    'join_type': 'inner',
                    'join_condition': match[1],
                    'description': f"Join with {match[0]} on {match[1]}"
                })
        
        info['complexity_score'] = self._calculate_complexity(content)
        
        return info
    
    def _analyze_shell_script_comprehensive(self, content: str) -> Dict[str, Any]:
        """Comprehensive analysis of Shell scripts"""
        info = {
            'input_tables': [],
            'output_tables': [],
            'data_sources': [],
            'data_targets': [],
            'transformations': [],
            'joins': [],
            'data_quality_rules': [],
            'business_rules': [],
            'column_mappings': [],
            'complexity_score': 0
        }
        
        # Extract file paths that might be data sources/targets
        patterns = [
            r'/user/[^/]+/[^/]+/[^/]+/([^/\s]+)',
            r'/data/[^/]+/[^/]+/([^/\s]+)',
            r'hdfs://[^/]+/[^/]+/([^/\s]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                if match not in info['data_sources']:
                    info['data_sources'].append(match)
                    info['data_targets'].append(match)
        
        info['complexity_score'] = self._calculate_complexity(content)
        
        return info
    
    def _find_script_file(self, script_path: str, workflow_file: Path) -> Optional[Path]:
        """Find the actual script file"""
        if not script_path or script_path == "email_notification":
            return None
            
        # Extract just the filename from the path
        script_name = Path(script_path).name
        
        # Try different search patterns
        patterns = [
            f"**/{script_name}",
            f"**/*{script_name}",
            f"**/{script_name}*"
        ]
        
        for pattern in patterns:
            matches = list(self.hadoop_repo_path.glob(pattern))
            if matches:
                return matches[0]
        
        return None
    
    def _extract_script_name(self, script_path: str) -> str:
        """Extract script name from script path"""
        if not script_path or script_path == "email_notification":
            return script_path or 'N/A'
        
        # Extract filename from path (handle variable substitution)
        if '/' in script_path:
            filename = script_path.split('/')[-1]
        else:
            filename = script_path
        
        return filename
    
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
    
    def _determine_business_purpose(self, workflow_name: str, actions: List[Dict[str, Any]]) -> str:
        """Determine overall business purpose of workflow"""
        workflow_lower = workflow_name.lower()
        
        if 'ingestion' in workflow_lower:
            return "Data Ingestion and Processing"
        elif 'xref' in workflow_lower:
            return "Cross-Reference Processing"
        elif 'audit' in workflow_lower:
            return "Audit and Compliance"
        elif 'sqoop' in workflow_lower:
            return "Data Extraction and Loading"
        else:
            return "Data Processing and Transformation"
    
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
    
    def _find_all_scripts(self) -> List[Path]:
        """Find all script files in the repository"""
        script_extensions = ['.py', '.pig', '.sql', '.sh', '.scala']
        script_files = []
        
        for ext in script_extensions:
            files = list(self.hadoop_repo_path.glob(f"**/*{ext}"))
            script_files.extend(files)
        
        print(f"   📄 Found {len(script_files)} total scripts")
        return script_files
    
    def _identify_used_scripts_enhanced(self, all_scripts: List[Path], workflow_analyses: List[Dict[str, Any]]):
        """ENHANCED: Identify which scripts are used vs unused with dependency analysis"""
        directly_used_scripts = set()
        
        # Extract script paths from workflow analyses
        for workflow in workflow_analyses:
            for action in workflow['actions']:
                script_path = action.get('script_path', '')
                if script_path:
                    # Try to find the actual script file
                    script_file = self._find_script_file(script_path, Path(workflow['file_path']))
                    if script_file:
                        directly_used_scripts.add(script_file)
        
        print(f"   📋 Direct workflow references: {len(directly_used_scripts)} scripts")
        
        # NEW: Analyze script-to-script dependencies
        print("   🔍 Analyzing script-to-script dependencies...")
        self._analyze_script_dependencies(all_scripts)
        
        # Find indirectly used scripts
        indirectly_used_scripts = set()
        for script_file in directly_used_scripts:
            indirectly_used_scripts.update(self._find_indirect_dependencies(script_file))
        
        # Combine direct and indirect usage
        self.used_scripts = directly_used_scripts | indirectly_used_scripts
        self.unused_scripts = set(all_scripts) - self.used_scripts
        
        print(f"   ✅ Directly used scripts: {len(directly_used_scripts)}")
        print(f"   ✅ Indirectly used scripts: {len(indirectly_used_scripts)}")
        print(f"   ✅ Total used scripts: {len(self.used_scripts)}")
        print(f"   ✅ Unused scripts: {len(self.unused_scripts)}")
    
    def _analyze_script_dependencies(self, all_scripts: List[Path]):
        """NEW: Analyze script-to-script dependencies"""
        self.script_dependencies = {}
        
        for script_file in all_scripts:
            if not script_file.exists():
                continue
                
            try:
                content = script_file.read_text()
                dependencies = self._extract_script_calls(content, script_file)
                
                if dependencies:
                    self.script_dependencies[script_file] = dependencies
                    
            except Exception as e:
                print(f"   ⚠️ Error analyzing dependencies for {script_file}: {e}")
    
    def _extract_script_calls(self, content: str, script_file: Path) -> List[Path]:
        """NEW: Extract script calls from content"""
        dependencies = []
        script_dir = script_file.parent
        
        # Python imports
        if script_file.suffix == '.py':
            import_patterns = [
                r'import\s+([a-zA-Z_][a-zA-Z0-9_]*)',
                r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+import',
                r'execfile\(["\']([^"\']+)["\']',
                r'exec\(open\(["\']([^"\']+)["\']'
            ]
            
            for pattern in import_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    # Look for corresponding .py files
                    for py_file in script_dir.glob(f"{match}.py"):
                        if py_file != script_file:
                            dependencies.append(py_file)
        
        # Shell script calls
        elif script_file.suffix == '.sh':
            shell_patterns = [
                r'source\s+([^\s]+)',
                r'\.\s+([^\s]+)',
                r'bash\s+([^\s]+)',
                r'sh\s+([^\s]+)'
            ]
            
            for pattern in shell_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    # Look for corresponding shell files
                    for sh_file in script_dir.glob(f"{match}*"):
                        if sh_file.suffix in ['.sh', '.bash'] and sh_file != script_file:
                            dependencies.append(sh_file)
        
        # Pig script calls
        elif script_file.suffix == '.pig':
            pig_patterns = [
                r'exec\s+["\']([^"\']+)["\']',
                r'run\s+["\']([^"\']+)["\']'
            ]
            
            for pattern in pig_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    # Look for corresponding pig files
                    for pig_file in script_dir.glob(f"{match}*"):
                        if pig_file.suffix == '.pig' and pig_file != script_file:
                            dependencies.append(pig_file)
        
        # SQL script calls
        elif script_file.suffix == '.sql':
            sql_patterns = [
                r'source\s+["\']([^"\']+)["\']',
                r'@([^\s]+)'
            ]
            
            for pattern in sql_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    # Look for corresponding sql files
                    for sql_file in script_dir.glob(f"{match}*"):
                        if sql_file.suffix == '.sql' and sql_file != script_file:
                            dependencies.append(sql_file)
        
        return dependencies
    
    def _find_indirect_dependencies(self, script_file: Path) -> Set[Path]:
        """NEW: Find all indirect dependencies recursively"""
        indirect_deps = set()
        visited = set()
        
        def _find_recursive(script: Path):
            if script in visited:
                return
            visited.add(script)
            
            if script in self.script_dependencies:
                for dep in self.script_dependencies[script]:
                    indirect_deps.add(dep)
                    _find_recursive(dep)
        
        _find_recursive(script_file)
        return indirect_deps
    
    def _analyze_business_domains(self, workflow_analyses: List[Dict[str, Any]]):
        """Analyze business domains"""
        domain_workflows = {}
        
        for workflow in workflow_analyses:
            domain = workflow['business_domain']
            if domain not in domain_workflows:
                domain_workflows[domain] = []
            domain_workflows[domain].append(workflow)
        
        print(f"   🏢 Business domains identified: {len(domain_workflows)}")
        for domain, workflows in domain_workflows.items():
            print(f"      {domain}: {len(workflows)} workflows")
        
        for domain, workflows in domain_workflows.items():
            all_technologies = set()
            all_input_tables = set()
            all_output_tables = set()
            total_scripts = 0
            
            for workflow in workflows:
                for tech in workflow['technologies'].keys():
                    all_technologies.add(tech)
                all_input_tables.update(workflow['input_tables'])
                all_output_tables.update(workflow['output_tables'])
                total_scripts += workflow['total_actions']
            
            self.business_domains[domain] = BusinessDomain(
                name=domain,
                workflows=[w['name'] for w in workflows],
                data_sources=list(all_input_tables),
                data_targets=list(all_output_tables),
                technologies=list(all_technologies),
                total_scripts=total_scripts,
                description=f"{domain} processing workflows"
            )
    
    def _create_pipeline_flows(self, workflow_analyses: List[Dict[str, Any]]):
        """Create pipeline flows"""
        print("   🔄 Pipeline flows created")
        for workflow in workflow_analyses:
            pipeline_flow = PipelineFlow(
                domain=workflow['business_domain'],
                coordinator_name=workflow['name'],
                workflow_name=workflow['name'],
                frequency=workflow['frequency'],
                execution_order=workflow['workflow_execution_order'],
                total_actions=workflow['total_actions'],
                technologies_used=workflow['technologies'],
                data_sources=workflow['data_sources'],
                data_targets=workflow['data_targets'],
                business_purpose=workflow['business_purpose']
            )
            self.pipeline_flows.append(pipeline_flow)
    
    def _create_execution_steps(self, workflow_analyses: List[Dict[str, Any]]):
        """Create execution steps"""
        print("   ⚡ Execution steps created")
        for workflow in workflow_analyses:
            for i, action in enumerate(workflow['actions']):
                execution_step = ExecutionStep(
                    workflow_name=workflow['name'],
                    step_number=i + 1,
                    action_name=action['action_name'],
                    technology=action['technology'],
                    script_name=action['script_name'],
                    execution_order=i + 1,
                    input_tables=action['input_tables'],
                    output_tables=action['output_tables'],
                    business_purpose=action['business_purpose'],
                    dependencies=[],
                    is_critical=action['action_name'] in workflow['critical_path']
                )
                self.execution_steps.append(execution_step)
    
    def _analyze_technology_flows(self):
        """Analyze technology flows"""
        print("   🔧 Technology flows analyzed")
        tech_scripts = {}
        
        for step in self.execution_steps:
            tech = step.technology
            if tech not in tech_scripts:
                tech_scripts[tech] = {
                    'scripts': [],
                    'input_sources': set(),
                    'output_targets': set(),
                    'transformation_types': set(),
                    'business_domains': set(),
                    'complexity_scores': []
                }
            
            tech_scripts[tech]['scripts'].append(step.script_name)
            tech_scripts[tech]['input_sources'].update(step.input_tables)
            tech_scripts[tech]['output_targets'].update(step.output_tables)
            tech_scripts[tech]['business_domains'].add(step.workflow_name)
        
        for tech, info in tech_scripts.items():
            self.technology_flows[tech] = TechnologyFlow(
                technology=tech,
                script_count=len(info['scripts']),
                input_sources=list(info['input_sources']),
                output_targets=list(info['output_targets']),
                transformation_types=list(info['transformation_types']),
                complexity_score=sum(info['complexity_scores']) // len(info['complexity_scores']) if info['complexity_scores'] else 0,
                business_domains=list(info['business_domains'])
            )
    
    def _create_detailed_data_flows(self):
        """Create detailed data flows"""
        print("   📊 Detailed data flows created")
        # This would require more detailed analysis of each script
        # For now, create basic data flow details
        for step in self.execution_steps:
            for input_table in step.input_tables:
                for output_table in step.output_tables:
                    data_flow_detail = DataFlowDetail(
                        source_table=input_table,
                        source_column='*',
                        transformation_logic=step.business_purpose,
                        target_table=output_table,
                        target_column='*',
                        business_rule='',
                        data_quality_rule='',
                        script_name=step.script_name,
                        technology=step.technology
                    )
                    self.data_flow_details.append(data_flow_detail)
    
    def _generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate comprehensive analysis report"""
        
        # Calculate statistics
        total_workflows = len(self.pipeline_flows)
        total_scripts = len(self.all_scripts)
        used_scripts_count = len(self.used_scripts)
        unused_scripts_count = len(self.unused_scripts)
        
        # Technology breakdown
        tech_breakdown = {}
        for tech_flow in self.technology_flows.values():
            tech_breakdown[tech_flow.technology] = tech_flow.script_count
        
        # Business domain analysis
        domain_analysis = {}
        for domain in self.business_domains.values():
            domain_analysis[domain.name] = len(domain.workflows)
        
        return {
            'summary': {
                'total_workflows': total_workflows,
                'total_scripts': total_scripts,
                'used_scripts_count': used_scripts_count,
                'unused_scripts_count': unused_scripts_count,
                'usage_percentage': (used_scripts_count / total_scripts * 100) if total_scripts > 0 else 0,
                'technology_breakdown': tech_breakdown,
                'domain_analysis': domain_analysis,
                'table_registry_size': len(self.table_registry),
                'script_dependencies_count': len(self.script_dependencies),
                'scripts_with_dependencies': len([s for s in self.all_scripts if s in self.script_dependencies])
            },
            'business_domains': [asdict(domain) for domain in self.business_domains.values()],
            'pipeline_flows': [asdict(flow) for flow in self.pipeline_flows],
            'execution_steps': [asdict(step) for step in self.execution_steps],
            'technology_flows': [asdict(flow) for flow in self.technology_flows.values()],
            'data_flow_details': [asdict(detail) for detail in self.data_flow_details],
            'unused_scripts': [str(script) for script in self.unused_scripts],
            'table_registry': self.table_registry,
            'script_dependencies': {str(k): [str(v) for v in vs] for k, vs in self.script_dependencies.items()}
        }
    
    def generate_comprehensive_excel_report(self, analysis: Dict[str, Any], 
                                          output_file: str = "comprehensive_hadoop_analysis.xlsx"):
        """Generate comprehensive Excel report with all mapping levels"""
        print(f"📊 Generating comprehensive Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Summary Sheet
            summary_data = [analysis['summary']]
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Business Domains Sheet
            domains_data = []
            for domain in analysis['business_domains']:
                domains_data.append({
                    'Business Domain': domain['name'],
                    'Workflows': ', '.join(domain['workflows']),
                    'Data Sources': ', '.join(domain['data_sources']),
                    'Data Targets': ', '.join(domain['data_targets']),
                    'Technologies': ', '.join(domain['technologies']),
                    'Total Scripts': domain['total_scripts'],
                    'Description': domain['description']
                })
            
            pd.DataFrame(domains_data).to_excel(writer, sheet_name='Business Domains', index=False)
            
            # Pipeline Flows Sheet
            pipeline_data = []
            for flow in analysis['pipeline_flows']:
                pipeline_data.append({
                    'Business Domain': flow['domain'],
                    'Coordinator Name': flow['coordinator_name'],
                    'Workflow Name': flow['workflow_name'],
                    'Frequency': flow['frequency'],
                    'Execution Order': flow['execution_order'],
                    'Total Actions': flow['total_actions'],
                    'Technologies Used': str(flow['technologies_used']),
                    'Data Sources': ', '.join(flow['data_sources']),
                    'Data Targets': ', '.join(flow['data_targets']),
                    'Business Purpose': flow['business_purpose']
                })
            
            pd.DataFrame(pipeline_data).to_excel(writer, sheet_name='Pipeline Flows', index=False)
            
            # Execution Steps Sheet
            steps_data = []
            for step in analysis['execution_steps']:
                steps_data.append({
                    'Workflow Name': step['workflow_name'],
                    'Step Number': step['step_number'],
                    'Action Name': step['action_name'],
                    'Technology': step['technology'],
                    'Script Name': step['script_name'],
                    'Execution Order': step['execution_order'],
                    'Input Tables': ', '.join(step['input_tables']),
                    'Output Tables': ', '.join(step['output_tables']),
                    'Business Purpose': step['business_purpose'],
                    'Dependencies': ', '.join(step['dependencies']),
                    'Is Critical': step['is_critical']
                })
            
            pd.DataFrame(steps_data).to_excel(writer, sheet_name='Execution Steps', index=False)
            
            # Technology Flows Sheet
            tech_data = []
            for tech in analysis['technology_flows']:
                tech_data.append({
                    'Technology': tech['technology'],
                    'Script Count': tech['script_count'],
                    'Input Sources': ', '.join(tech['input_sources']),
                    'Output Targets': ', '.join(tech['output_targets']),
                    'Transformation Types': ', '.join(tech['transformation_types']),
                    'Complexity Score': tech['complexity_score'],
                    'Business Domains': ', '.join(tech['business_domains'])
                })
            
            pd.DataFrame(tech_data).to_excel(writer, sheet_name='Technology Flows', index=False)
            
            # Data Flow Details Sheet
            data_flow_data = []
            for detail in analysis['data_flow_details']:
                data_flow_data.append({
                    'Source Table': detail['source_table'],
                    'Source Column': detail['source_column'],
                    'Transformation Logic': detail['transformation_logic'],
                    'Target Table': detail['target_table'],
                    'Target Column': detail['target_column'],
                    'Business Rule': detail['business_rule'],
                    'Data Quality Rule': detail['data_quality_rule'],
                    'Script Name': detail['script_name'],
                    'Technology': detail['technology']
                })
            
            pd.DataFrame(data_flow_data).to_excel(writer, sheet_name='Data Flow Details', index=False)
            
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
        
        print(f"✅ Comprehensive Excel report generated: {output_file}")

def analyze_single_repository(repo_path: str) -> str:
    """Analyze a single repository and return the output file path"""
    repo_name = Path(repo_path).name
    print(f"\n📁 Analyzing: {repo_path}")
    print("-" * 50)
    
    try:
        # Initialize analyzer
        analyzer = ComprehensiveHadoopAnalyzer(repo_path)
        
        # Perform comprehensive analysis
        analysis = analyzer.analyze_hadoop_repository()
        
        # Generate Excel report with repo name
        output_file = f"comprehensive_hadoop_analysis_{repo_name}.xlsx"
        analyzer.generate_comprehensive_excel_report(analysis, output_file)
        
        # Print summary
        summary = analysis['summary']
        print(f"\n📊 Comprehensive Analysis Results!")
        print(f"   Total Workflows: {summary['total_workflows']}")
        print(f"   Total Scripts: {summary['total_scripts']}")
        print(f"   Used Scripts: {summary['used_scripts_count']}")
        print(f"   Unused Scripts: {summary['unused_scripts_count']}")
        print(f"   Usage: {summary['usage_percentage']:.1f}%")
        print(f"   Script Dependencies Found: {summary['script_dependencies_count']}")
        print(f"   Scripts with Dependencies: {summary['scripts_with_dependencies']}")
        
        print(f"\n🔧 Technology Breakdown:")
        for tech, count in summary['technology_breakdown'].items():
            print(f"   {tech.upper()}: {count} scripts")
        
        print(f"\n🏢 Business Domains:")
        for domain, count in summary['domain_analysis'].items():
            print(f"   {domain}: {count} workflows")
        
        return output_file
        
    except Exception as e:
        print(f"❌ Error analyzing {repo_path}: {e}")
        raise

def main():
    """Main function - UPGRADED with multi-repo support"""
    print("🚀 Comprehensive Hadoop Repository Analysis Tool - UPGRADED")
    print("=" * 70)
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python comprehensive_hadoop_analyzer_upgraded.py <hadoop_path1> [hadoop_path2] ...")
        print("   OR: python comprehensive_hadoop_analyzer_upgraded.py --file <file_with_paths>")
        print("\nExamples:")
        print("python comprehensive_hadoop_analyzer_upgraded.py /path/to/repo1 /path/to/repo2")
        print("python comprehensive_hadoop_analyzer_upgraded.py --file repo_list.txt")
        print("\nOr run with default test path:")
        print("python comprehensive_hadoop_analyzer_upgraded.py")
        
        # Use default test path
        repo_paths = ["./OneDrive_1_7-25-2025/Hadoop/app-data-ingestion"]
        print(f"\n🔧 Using default test path: {repo_paths[0]}")
    else:
        if sys.argv[1] == "--file" and len(sys.argv) > 2:
            # Read repository paths from file
            file_path = sys.argv[2]
            try:
                with open(file_path, 'r') as f:
                    repo_paths = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                print(f"📄 Reading repository paths from: {file_path}")
            except Exception as e:
                print(f"❌ Error reading file {file_path}: {e}")
                return
        else:
            # Repository paths from command line arguments
            repo_paths = sys.argv[1:]
    
    if not repo_paths:
        print("❌ Please provide Hadoop repository path(s)")
        return
    
    print(f"🚀 Analyzing {len(repo_paths)} repositories...")
    print("=" * 70)
    
    # Analyze each repository
    successful_analyses = []
    failed_analyses = []
    
    for i, repo_path in enumerate(repo_paths, 1):
        print(f"\n📁 [{i}/{len(repo_paths)}] Analyzing: {repo_path}")
        print("-" * 50)
        
        try:
            output_file = analyze_single_repository(repo_path)
            successful_analyses.append((repo_path, output_file))
            print(f"✅ [{i}/{len(repo_paths)}] Completed: {output_file}")
            
        except Exception as e:
            failed_analyses.append((repo_path, str(e)))
            print(f"❌ [{i}/{len(repo_paths)}] Failed: {e}")
    
    # Print final summary
    print(f"\n🎯 COMPREHENSIVE ANALYSIS SUMMARY")
    print("=" * 70)
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
    print("   1. High-level business flow")
    print("   2. Pipeline-level flow")
    print("   3. Execution order flow")
    print("   4. Technology-specific flow")
    print("   5. Granular detail flow")
    print("   6. Script dependency analysis (NEW)")
    print("   7. Multi-repository support (NEW)")
    
    # Exit with error code if any failed
    if failed_analyses:
        sys.exit(1)

if __name__ == "__main__":
    main()
