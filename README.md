#!/usr/bin/env python3
"""
Hadoop Oozie Workflow Mapper

This script parses Hadoop repositories and Oozie workflows to extract source-target mappings
for data lineage analysis and migration planning.

Author: AI Assistant
Version: 1.0
"""

import argparse
import os
import sys
import logging
import xml.etree.ElementTree as ET
import re
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
import pandas as pd
import google.generativeai as genai
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hadoop_mapper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class FieldMapping:
    """Data class for field mapping information"""
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

class OozieWorkflowParser:
    """Parser for Oozie workflow XML files"""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.workflow_files = []
        self.actions = []
        
    def find_workflow_files(self, workflow_name: str) -> List[Path]:
        """Recursively search for workflow XML files matching the given name"""
        workflow_files = []
        
        for root, dirs, files in os.walk(self.repo_path):
            for file in files:
                if file == 'workflow.xml' and workflow_name in root:
                    workflow_files.append(Path(root) / file)
                    logger.info(f"Found workflow file: {Path(root) / file}")
        
        return workflow_files
    
    def parse_workflow_xml(self, workflow_file: Path) -> Dict[str, Any]:
        """Parse Oozie workflow XML and extract action information"""
        try:
            tree = ET.parse(workflow_file)
            root = tree.getroot()
            
            workflow_info = {
                'file_path': str(workflow_file),
                'workflow_name': workflow_file.parent.name,
                'actions': [],
                'transitions': {}
            }
            
            # Extract actions
            for action in root.findall('.//action'):
                action_name = action.get('name', '')
                action_type = None
                script_path = None
                config = {}
                
                # Determine action type and extract relevant information
                for child in action:
                    if child.tag in ['hive', 'pig', 'spark', 'pyspark', 'shell']:
                        action_type = child.tag
                        
                        # Extract script path
                        script_elem = child.find('script')
                        if script_elem is not None:
                            script_path = script_elem.text
                        
                        # Extract configuration
                        config_elem = child.find('configuration')
                        if config_elem is not None:
                            for prop in config_elem.findall('property'):
                                name = prop.find('name')
                                value = prop.find('value')
                                if name is not None and value is not None:
                                    config[name.text] = value.text
                        
                        break
                
                # Extract transitions
                transitions = {}
                start_elem = action.find('start')
                if start_elem is not None:
                    transitions['start'] = start_elem.text
                
                ok_elem = action.find('ok')
                if ok_elem is not None:
                    transitions['ok'] = ok_elem.get('to', '')
                
                error_elem = action.find('error')
                if error_elem is not None:
                    transitions['error'] = error_elem.get('to', '')
                
                action_info = {
                    'name': action_name,
                    'type': action_type,
                    'script_path': script_path,
                    'config': config,
                    'transitions': transitions
                }
                
                workflow_info['actions'].append(action_info)
                workflow_info['transitions'][action_name] = transitions
            
            logger.info(f"Parsed workflow {workflow_file.parent.name}: {len(workflow_info['actions'])} actions found")
            return workflow_info
            
        except ET.ParseError as e:
            logger.error(f"Error parsing XML file {workflow_file}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing {workflow_file}: {e}")
            return None

class ScriptAnalyzer:
    """Analyzer for SQL and script files"""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        
    def read_script_content(self, script_path: str) -> Optional[str]:
        """Read script content from local file or log HDFS path"""
        if script_path.startswith('hdfs://') or script_path.startswith('/'):
            logger.warning(f"HDFS path detected (skipping): {script_path}")
            return None
        
        # Try to find the script in the repository
        script_file = self.repo_path / script_path
        if script_file.exists():
            try:
                with open(script_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                logger.info(f"Read script content from: {script_file}")
                return content
            except Exception as e:
                logger.error(f"Error reading script {script_file}: {e}")
                return None
        else:
            logger.warning(f"Script file not found: {script_file}")
            return None
    
    def extract_source_tables(self, content: str) -> List[str]:
        """Extract source table names from SQL content"""
        tables = set()
        
        # Patterns for different SQL dialects
        patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'from\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'join\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.update(matches)
        
        return list(tables)
    
    def extract_target_table(self, content: str) -> Optional[str]:
        """Extract target table name from SQL content"""
        patterns = [
            r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'CREATE\s+OR\s+REPLACE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def extract_field_mappings(self, content: str) -> List[Dict[str, str]]:
        """Extract field mappings from SQL content"""
        mappings = []
        
        # Look for SELECT statements with column mappings
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        select_match = re.search(select_pattern, content, re.IGNORECASE | re.DOTALL)
        
        if select_match:
            select_clause = select_match.group(1)
            
            # Split by comma and analyze each field
            fields = [field.strip() for field in select_clause.split(',')]
            
            for i, field in enumerate(fields):
                field_info = {
                    'source_field': field,
                    'target_field': field,
                    'transformation': '',
                    'data_type': 'VARCHAR'  # Default, could be enhanced
                }
                
                # Check for aliases
                if ' AS ' in field.upper():
                    parts = field.split(' AS ', 1)
                    field_info['source_field'] = parts[0].strip()
                    field_info['target_field'] = parts[1].strip()
                
                # Check for transformations
                if any(func in field.upper() for func in ['CASE', 'CONCAT', 'SUBSTRING', 'UPPER', 'LOWER']):
                    field_info['transformation'] = field
                
                mappings.append(field_info)
        
        return mappings

class AIPoweredMapper:
    """AI-powered mapping using Gemini API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
    
    def analyze_complex_script(self, script_content: str, script_type: str) -> Dict[str, Any]:
        """Use AI to analyze complex scripts and extract mappings"""
        try:
            prompt = f"""
            Analyze this {script_type} script and extract the following information:
            
            1. Source datasets (tables in FROM/JOIN clauses)
            2. Target dataset (INSERT INTO / CREATE TABLE)
            3. Field mappings between source and target
            4. Data transformations (CASE statements, CONCAT, etc.)
            5. Data types and constraints
            
            Script content:
            {script_content}
            
            Please provide a structured JSON response with the following format:
            {{
                "source_tables": ["table1", "table2"],
                "target_table": "target_table",
                "field_mappings": [
                    {{
                        "source_field": "source_col",
                        "target_field": "target_col",
                        "transformation": "CASE WHEN ...",
                        "data_type": "VARCHAR(100)"
                    }}
                ],
                "transformations": ["transformation1", "transformation2"]
            }}
            """
            
            response = self.model.generate_content(prompt)
            
            # Try to parse JSON response
            try:
                result = json.loads(response.text)
                logger.info("AI analysis completed successfully")
                return result
            except json.JSONDecodeError:
                logger.warning("AI response could not be parsed as JSON")
                return self._fallback_analysis(script_content)
                
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return self._fallback_analysis(script_content)
    
    def _fallback_analysis(self, script_content: str) -> Dict[str, Any]:
        """Fallback analysis when AI fails"""
        return {
            "source_tables": [],
            "target_table": None,
            "field_mappings": [],
            "transformations": []
        }

class HadoopOozieMapper:
    """Main class for Hadoop Oozie mapping"""
    
    def __init__(self, repo_path: str, api_key: Optional[str] = None):
        self.repo_path = Path(repo_path)
        self.parser = OozieWorkflowParser(repo_path)
        self.analyzer = ScriptAnalyzer(repo_path)
        self.ai_mapper = AIPoweredMapper(api_key) if api_key else None
        self.mappings = []
        
    def process_workflow(self, workflow_name: str) -> List[FieldMapping]:
        """Process a single workflow and extract mappings"""
        logger.info(f"Processing workflow: {workflow_name}")
        
        # Find workflow files
        workflow_files = self.parser.find_workflow_files(workflow_name)
        
        if not workflow_files:
            logger.warning(f"No workflow files found for: {workflow_name}")
            return []
        
        all_mappings = []
        
        for workflow_file in workflow_files:
            # Parse workflow XML
            workflow_info = self.parser.parse_workflow_xml(workflow_file)
            
            if not workflow_info:
                continue
            
            # Process actions in order
            action_order = self._determine_action_order(workflow_info)
            
            for order, action in enumerate(action_order):
                mappings = self._process_action(action, order + 1, workflow_info)
                all_mappings.extend(mappings)
        
        return all_mappings
    
    def _determine_action_order(self, workflow_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Determine the processing order of actions based on transitions"""
        actions = workflow_info['actions']
        transitions = workflow_info['transitions']
        
        # Find start action
        start_action = None
        for action in actions:
            if transitions.get(action['name'], {}).get('start'):
                start_action = action
                break
        
        if not start_action:
            logger.warning("No start action found, using first action")
            return actions
        
        # Build execution order
        ordered_actions = []
        current_action = start_action
        visited = set()
        
        while current_action and current_action['name'] not in visited:
            visited.add(current_action['name'])
            ordered_actions.append(current_action)
            
            # Find next action
            next_action_name = transitions.get(current_action['name'], {}).get('ok')
            if next_action_name:
                current_action = next((a for a in actions if a['name'] == next_action_name), None)
            else:
                break
        
        return ordered_actions
    
    def _process_action(self, action: Dict[str, Any], order: int, workflow_info: Dict[str, Any]) -> List[FieldMapping]:
        """Process a single action and extract field mappings"""
        mappings = []
        
        if not action['script_path']:
            logger.warning(f"No script path found for action: {action['name']}")
            return mappings
        
        # Read script content
        script_content = self.analyzer.read_script_content(action['script_path'])
        
        if not script_content:
            return mappings
        
        # Analyze script
        if self.ai_mapper and len(script_content) > 1000:  # Use AI for complex scripts
            analysis = self.ai_mapper.analyze_complex_script(script_content, action['type'])
        else:
            analysis = self._heuristic_analysis(script_content)
        
        # Convert analysis to FieldMapping objects
        for i, field_mapping in enumerate(analysis.get('field_mappings', [])):
            mapping = FieldMapping(
                id=f"{workflow_info['workflow_name']}_{action['name']}_{i+1}",
                partner="Unknown",
                schema="default",
                target_table_name=analysis.get('target_table', 'unknown'),
                target_field_name=field_mapping.get('target_field', 'unknown'),
                target_field_data_type=field_mapping.get('data_type', 'VARCHAR'),
                pk=False,
                contains_pii=False,
                field_type="data",
                field_depends_on=field_mapping.get('source_field', ''),
                processing_order=order,
                pre_processing_rules=field_mapping.get('transformation', ''),
                source_field_names=field_mapping.get('source_field', ''),
                source_dataset_name=', '.join(analysis.get('source_tables', [])),
                field_definition=field_mapping.get('transformation', ''),
                example_1="",
                example_2=""
            )
            mappings.append(mapping)
        
        return mappings
    
    def _heuristic_analysis(self, script_content: str) -> Dict[str, Any]:
        """Heuristic analysis of script content"""
        source_tables = self.analyzer.extract_source_tables(script_content)
        target_table = self.analyzer.extract_target_table(script_content)
        field_mappings = self.analyzer.extract_field_mappings(script_content)
        
        return {
            'source_tables': source_tables,
            'target_table': target_table,
            'field_mappings': field_mappings,
            'transformations': []
        }
    
    def generate_excel_output(self, mappings: List[FieldMapping], output_path: str):
        """Generate Excel output with field mappings"""
        if not mappings:
            logger.warning("No mappings to export")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame([asdict(mapping) for mapping in mappings])
        
        # Write to Excel
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Field_Mappings', index=False)
            
            # Add summary sheet
            summary_data = {
                'Metric': ['Total Mappings', 'Unique Target Tables', 'Unique Source Tables', 'Workflows Processed'],
                'Value': [
                    len(mappings),
                    df['target_table_name'].nunique(),
                    df['source_dataset_name'].nunique(),
                    df['id'].str.split('_').str[0].nunique()
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"Excel output generated: {output_path}")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Hadoop Oozie Workflow Mapper')
    parser.add_argument('repo_path', help='Path to Hadoop repository')
    parser.add_argument('workflow_name', help='Name of Oozie workflow to process')
    parser.add_argument('--output', '-o', help='Output Excel file path', 
                       default=f'hadoop_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
    parser.add_argument('--api-key', help='Gemini API key for AI-powered analysis')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate inputs
    if not os.path.exists(args.repo_path):
        logger.error(f"Repository path does not exist: {args.repo_path}")
        sys.exit(1)
    
    # Initialize mapper
    mapper = HadoopOozieMapper(args.repo_path, args.api_key)
    
    # Process workflow
    logger.info(f"Starting analysis of workflow: {args.workflow_name}")
    mappings = mapper.process_workflow(args.workflow_name)
    
    if mappings:
        logger.info(f"Found {len(mappings)} field mappings")
        
        # Generate Excel output
        mapper.generate_excel_output(mappings, args.output)
        logger.info(f"Analysis complete. Output saved to: {args.output}")
    else:
        logger.warning("No mappings found")
    
    logger.info("Analysis completed")

if __name__ == "__main__":
    main()
