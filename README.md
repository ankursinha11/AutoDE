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

@dataclass
class PipelineAction:
    """Data class for pipeline action information"""
    action_name: str
    action_type: str
    processing_order: int
    script_path: str
    input_parameters: str
    output_parameters: str
    dependencies: str
    error_handling: str
    configuration: str

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
        """Use AI to analyze complex scripts and extract comprehensive mappings"""
        try:
            prompt = f"""
            You are a data engineering expert analyzing a {script_type} script for source-target mapping.
            
            Analyze this script thoroughly and extract ALL the following information:
            
            1. Source datasets (tables in FROM/JOIN clauses) - be comprehensive
            2. Target dataset (INSERT INTO / CREATE TABLE / CREATE OR REPLACE TABLE)
            3. Complete field mappings between source and target with transformations
            4. Data transformations (CASE statements, CONCAT, SUBSTRING, UPPER, LOWER, COALESCE, etc.)
            5. Data types and constraints (VARCHAR, INT, DECIMAL, DATE, etc.)
            6. JOIN conditions and relationships
            7. WHERE clauses and filters
            8. Aggregations (SUM, COUNT, AVG, etc.)
            9. Window functions (ROW_NUMBER, RANK, etc.)
            10. Business logic and calculations
            
            Script content:
            {script_content}
            
            Provide a detailed JSON response with this EXACT format:
            {{
                "source_tables": ["schema.table1", "schema.table2"],
                "target_table": "schema.target_table",
                "field_mappings": [
                    {{
                        "source_field": "source_col1",
                        "target_field": "target_col1", 
                        "transformation": "CASE WHEN source_col1 IS NULL THEN 'Unknown' ELSE source_col1 END",
                        "data_type": "VARCHAR(100)",
                        "business_logic": "Null handling with default value",
                        "join_condition": "LEFT JOIN table2 ON table1.id = table2.id"
                    }},
                    {{
                        "source_field": "col1, col2",
                        "target_field": "concatenated_field",
                        "transformation": "CONCAT(col1, '-', col2)",
                        "data_type": "VARCHAR(200)",
                        "business_logic": "Concatenate two fields with separator",
                        "join_condition": ""
                    }}
                ],
                "transformations": ["CASE statements", "CONCAT operations", "Date formatting"],
                "joins": ["LEFT JOIN table2 ON condition", "INNER JOIN table3 ON condition"],
                "filters": ["WHERE status = 'ACTIVE'", "AND created_date > '2023-01-01'"],
                "aggregations": ["SUM(amount)", "COUNT(DISTINCT user_id)"],
                "business_rules": ["Data validation", "Status mapping", "Date calculations"]
            }}
            
            Be extremely thorough and extract EVERY field mapping and transformation.
            """
            
            response = self.model.generate_content(prompt)
            
            # Try to parse JSON response
            try:
                result = json.loads(response.text)
                logger.info("AI analysis completed successfully")
                return result
            except json.JSONDecodeError:
                logger.warning("AI response could not be parsed as JSON, trying to extract manually")
                return self._extract_from_text_response(response.text, script_content)
                
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return self._fallback_analysis(script_content)
    
    def _extract_from_text_response(self, response_text: str, script_content: str) -> Dict[str, Any]:
        """Extract information from AI text response when JSON parsing fails"""
        # Try to find JSON-like content in the response
        import re
        
        # Look for JSON patterns in the response
        json_pattern = r'\{.*\}'
        json_match = re.search(json_pattern, response_text, re.DOTALL)
        
        if json_match:
            try:
                return json.loads(json_match.group())
            except:
                pass
        
        # Fallback to heuristic analysis
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
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.parser = OozieWorkflowParser(repo_path)
        self.analyzer = ScriptAnalyzer(repo_path)
        # Hardcoded Gemini API key
        self.ai_mapper = AIPoweredMapper("AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA")
        self.mappings = []
        self.pipeline_actions = []
        
    def process_workflow(self, workflow_name: str) -> Tuple[List[FieldMapping], List[PipelineAction]]:
        """Process a single workflow and extract mappings and pipeline actions"""
        logger.info(f"Processing workflow: {workflow_name}")
        
        # Find workflow files
        workflow_files = self.parser.find_workflow_files(workflow_name)
        
        if not workflow_files:
            logger.warning(f"No workflow files found for: {workflow_name}")
            return [], []
        
        all_mappings = []
        all_actions = []
        
        for workflow_file in workflow_files:
            # Parse workflow XML
            workflow_info = self.parser.parse_workflow_xml(workflow_file)
            
            if not workflow_info:
                continue
            
            # Process actions in order
            action_order = self._determine_action_order(workflow_info)
            
            for order, action in enumerate(action_order):
                # Extract pipeline action information
                pipeline_action = self._extract_pipeline_action(action, order + 1, workflow_info)
                all_actions.append(pipeline_action)
                
                # Extract field mappings using AI
                mappings = self._process_action_with_ai(action, order + 1, workflow_info)
                all_mappings.extend(mappings)
        
        return all_mappings, all_actions
    
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
    
    def _extract_pipeline_action(self, action: Dict[str, Any], order: int, workflow_info: Dict[str, Any]) -> PipelineAction:
        """Extract pipeline action information"""
        # Extract input/output parameters from configuration
        input_params = []
        output_params = []
        
        for key, value in action.get('config', {}).items():
            if 'input' in key.lower() or 'source' in key.lower():
                input_params.append(f"{key}={value}")
            elif 'output' in key.lower() or 'target' in key.lower():
                output_params.append(f"{key}={value}")
        
        # Extract dependencies and error handling
        transitions = action.get('transitions', {})
        dependencies = transitions.get('ok', '')
        error_handling = transitions.get('error', '')
        
        # Extract configuration as string
        config_str = ', '.join([f"{k}={v}" for k, v in action.get('config', {}).items()])
        
        return PipelineAction(
            action_name=action['name'],
            action_type=action['type'] or 'unknown',
            processing_order=order,
            script_path=action['script_path'] or '',
            input_parameters='; '.join(input_params),
            output_parameters='; '.join(output_params),
            dependencies=dependencies,
            error_handling=error_handling,
            configuration=config_str
        )
    
    def _process_action_with_ai(self, action: Dict[str, Any], order: int, workflow_info: Dict[str, Any]) -> List[FieldMapping]:
        """Process a single action and extract field mappings using AI"""
        mappings = []
        
        if not action['script_path']:
            logger.warning(f"No script path found for action: {action['name']}")
            return mappings
        
        # Read script content
        script_content = self.analyzer.read_script_content(action['script_path'])
        
        if not script_content:
            return mappings
        
        logger.info(f"Analyzing script with AI for action: {action['name']}")
        
        # Always use AI analysis (mandatory)
        analysis = self.ai_mapper.analyze_complex_script(script_content, action['type'])
        
        # Convert analysis to FieldMapping objects
        for i, field_mapping in enumerate(analysis.get('field_mappings', [])):
            # Determine if field contains PII (basic heuristics)
            contains_pii = self._detect_pii(field_mapping.get('target_field', ''))
            
            # Determine if field is primary key
            is_pk = self._detect_primary_key(field_mapping.get('target_field', ''))
            
            mapping = FieldMapping(
                id=f"{workflow_info['workflow_name']}_{action['name']}_{i+1}",
                partner="Data Pipeline",
                schema=self._extract_schema(analysis.get('target_table', 'unknown')),
                target_table_name=analysis.get('target_table', 'unknown'),
                target_field_name=field_mapping.get('target_field', 'unknown'),
                target_field_data_type=field_mapping.get('data_type', 'VARCHAR'),
                pk=is_pk,
                contains_pii=contains_pii,
                field_type="data",
                field_depends_on=field_mapping.get('source_field', ''),
                processing_order=order,
                pre_processing_rules=field_mapping.get('transformation', ''),
                source_field_names=field_mapping.get('source_field', ''),
                source_dataset_name=', '.join(analysis.get('source_tables', [])),
                field_definition=field_mapping.get('business_logic', ''),
                example_1=self._generate_example(field_mapping.get('transformation', '')),
                example_2=self._generate_example2(field_mapping.get('transformation', ''))
            )
            mappings.append(mapping)
        
        return mappings
    
    def _detect_pii(self, field_name: str) -> bool:
        """Detect if field contains PII"""
        pii_keywords = ['ssn', 'social', 'email', 'phone', 'address', 'name', 'dob', 'birth', 'credit', 'card']
        return any(keyword in field_name.lower() for keyword in pii_keywords)
    
    def _detect_primary_key(self, field_name: str) -> bool:
        """Detect if field is a primary key"""
        pk_keywords = ['id', 'key', 'pk', 'primary']
        return any(keyword in field_name.lower() for keyword in pk_keywords)
    
    def _extract_schema(self, table_name: str) -> str:
        """Extract schema from table name"""
        if '.' in table_name:
            return table_name.split('.')[0]
        return 'default'
    
    def _generate_example(self, transformation: str) -> str:
        """Generate example value based on transformation"""
        if 'CASE' in transformation.upper():
            return 'Conditional value'
        elif 'CONCAT' in transformation.upper():
            return 'Concatenated string'
        elif 'SUM' in transformation.upper():
            return '12345.67'
        elif 'COUNT' in transformation.upper():
            return '100'
        else:
            return 'Sample value'
    
    def _generate_example2(self, transformation: str) -> str:
        """Generate second example value"""
        if 'DATE' in transformation.upper():
            return '2023-12-31'
        elif 'UPPER' in transformation.upper():
            return 'UPPERCASE'
        elif 'LOWER' in transformation.upper():
            return 'lowercase'
        else:
            return 'Another sample'
    
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
    
    def generate_excel_output(self, mappings: List[FieldMapping], pipeline_actions: List[PipelineAction], output_path: str):
        """Generate Excel output with 2 sheets: Pipeline Flow and STTM Mapping"""
        if not mappings and not pipeline_actions:
            logger.warning("No data to export")
            return
        
        # Create Excel writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Sheet 1: Pipeline Flow
            if pipeline_actions:
                pipeline_df = pd.DataFrame([asdict(action) for action in pipeline_actions])
                pipeline_df.to_excel(writer, sheet_name='Pipeline_Flow', index=False)
                logger.info(f"Pipeline Flow sheet created with {len(pipeline_actions)} actions")
            
            # Sheet 2: STTM Mapping
            if mappings:
                sttm_df = pd.DataFrame([asdict(mapping) for mapping in mappings])
                sttm_df.to_excel(writer, sheet_name='STTM_Mapping', index=False)
                logger.info(f"STTM Mapping sheet created with {len(mappings)} field mappings")
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total Pipeline Actions',
                    'Total Field Mappings', 
                    'Unique Target Tables',
                    'Unique Source Tables',
                    'Actions with PII Fields',
                    'Primary Key Fields'
                ],
                'Value': [
                    len(pipeline_actions),
                    len(mappings),
                    len(set(m.target_table_name for m in mappings)) if mappings else 0,
                    len(set(m.source_dataset_name for m in mappings)) if mappings else 0,
                    len([m for m in mappings if m.contains_pii]) if mappings else 0,
                    len([m for m in mappings if m.pk]) if mappings else 0
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"Excel output generated: {output_path}")
        logger.info(f"  - Pipeline Flow: {len(pipeline_actions)} actions")
        logger.info(f"  - STTM Mapping: {len(mappings)} field mappings")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Hadoop Oozie Workflow Mapper with AI Analysis')
    parser.add_argument('repo_path', help='Path to Hadoop repository')
    parser.add_argument('workflow_name', help='Name of Oozie workflow to process')
    parser.add_argument('--output', '-o', help='Output Excel file path', 
                       default=f'hadoop_pipeline_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate inputs
    if not os.path.exists(args.repo_path):
        logger.error(f"Repository path does not exist: {args.repo_path}")
        sys.exit(1)
    
    try:
        # Initialize mapper (API key is hardcoded)
        logger.info(f"Initializing AI-powered mapper for repository: {args.repo_path}")
        mapper = HadoopOozieMapper(args.repo_path)
        
        # Process workflow
        logger.info(f"Starting AI analysis of workflow: {args.workflow_name}")
        mappings, pipeline_actions = mapper.process_workflow(args.workflow_name)
        
        if mappings or pipeline_actions:
            logger.info(f"Analysis complete:")
            logger.info(f"  - Found {len(pipeline_actions)} pipeline actions")
            logger.info(f"  - Found {len(mappings)} field mappings")
            
            # Generate Excel output with 2 sheets
            mapper.generate_excel_output(mappings, pipeline_actions, args.output)
            logger.info(f"✅ Analysis complete. Output saved to: {args.output}")
            logger.info(f"📊 Excel file contains:")
            logger.info(f"   - Sheet 1: Pipeline Flow ({len(pipeline_actions)} actions)")
            logger.info(f"   - Sheet 2: STTM Mapping ({len(mappings)} field mappings)")
            logger.info(f"   - Sheet 3: Summary")
        else:
            logger.warning("No data found")
            logger.warning("This could mean:")
            logger.warning("  - Workflow name not found in repository")
            logger.warning("  - No workflow.xml files found")
            logger.warning("  - Script files are missing or inaccessible")
    
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    logger.info("Analysis completed successfully")

if __name__ == "__main__":
    main()
