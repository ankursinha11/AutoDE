#!/usr/bin/env python3
"""
Complete AI-Powered Source-to-Target Mapping Tool
=================================================

This single script does everything:
- Installs dependencies
- Tests Gemini connection
- Analyzes Hadoop and Databricks repositories
- Generates Excel reports with field mappings
- Compares implementations

Your Configuration:
- Gemini API Key: AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA
- Hadoop Path: ./app-cdd
- Databricks Path: ./CDD
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
    technology: str
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
    repository_type: str
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
{code_content[:2000]}
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

class RepositoryAnalyzer:
    """Unified repository analyzer for both Hadoop and Databricks"""
    
    def __init__(self, repo_path: str, ai_analyzer: GeminiAnalyzer, repo_type: str):
        self.repo_path = Path(repo_path)
        self.ai_analyzer = ai_analyzer
        self.repo_type = repo_type
        self.field_id_counter = 1
    
    def analyze_repository(self, repo_name: str) -> RepositoryAnalysis:
        """Analyze repository and generate field mappings"""
        print(f"🔍 Analyzing {self.repo_type.upper()} Repository: {repo_name}")
        
        # Find all script files
        script_files = self._find_all_scripts()
        print(f"📋 Found {len(script_files)} script files")
        
        # Analyze tables and generate field mappings
        tables = self._analyze_tables_with_ai(script_files)
        
        # Calculate statistics
        total_fields = sum(len(table.field_mappings) for table in tables)
        pii_fields = sum(len([f for f in table.field_mappings if f.contains_pii]) for table in tables)
        primary_keys = sum(len([f for f in table.field_mappings if f.pk]) for table in tables)
        
        return RepositoryAnalysis(
            repository_name=repo_name,
            repository_type=self.repo_type,
            tables=tables,
            total_tables=len(tables),
            total_fields=total_fields,
            pii_fields=pii_fields,
            primary_keys=primary_keys
        )
    
    def _find_all_scripts(self) -> List[Path]:
        """Find all script files in the repository"""
        script_extensions = ['.py', '.pig', '.sql', '.sh', '.scala', '.r']
        script_files = []
        
        for ext in script_extensions:
            files = list(self.repo_path.glob(f"**/*{ext}"))
            script_files.extend(files)
        
        return script_files
    
    def _analyze_tables_with_ai(self, script_files: List[Path]) -> List[TableAnalysis]:
        """Analyze tables using AI to generate field mappings"""
        tables = []
        table_analysis_map = {}
        
        for script_file in script_files:
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
                    repository=self.repo_type,
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
            return 'hive' if self.repo_type == 'hadoop' else 'sql'
        elif ext == '.sh':
            return 'shell'
        elif ext == '.scala':
            return 'spark'
        elif ext == '.r':
            return 'r'
        else:
            return 'unknown'

class ComparisonEngine:
    """Engine to compare Hadoop and Databricks implementations"""
    
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

def test_gemini_connection():
    """Test Gemini connection"""
    print("🔌 Testing Gemini Connection...")
    
    try:
        api_key = "AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA"
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-pro')
        response = model.generate_content("Hello, this is a test.")
        print("✅ Gemini connection successful!")
        return True
    except Exception as e:
        print(f"❌ Gemini connection failed: {e}")
        return False

def main():
    """Main function - Complete analysis in one script"""
    print("🚀 AI-Powered Source-to-Target Mapping Tool")
    print("=" * 60)
    print("📋 Your Configuration:")
    print("   Gemini API Key: AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA")
    print("   Hadoop Path: ./app-cdd")
    print("   Databricks Path: ./CDD")
    print("=" * 60)
    
    # Test Gemini connection
    if not test_gemini_connection():
        print("❌ Gemini connection failed. Please check your internet connection.")
        return
    
    # Check directories
    print("\n📁 Checking repository directories...")
    if not os.path.exists("./app-cdd"):
        print("❌ Hadoop repository not found: ./app-cdd")
        print("   Please make sure the app-cdd directory exists")
        return
    
    if not os.path.exists("./CDD"):
        print("❌ Databricks repository not found: ./CDD")
        print("   Please make sure the CDD directory exists")
        return
    
    print("✅ Repository directories found!")
    
    try:
        # Initialize components
        print("\n🔧 Initializing AI-Powered Mapper...")
        ai_analyzer = GeminiAnalyzer("AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA")
        hadoop_analyzer = RepositoryAnalyzer("./app-cdd", ai_analyzer, "hadoop")
        databricks_analyzer = RepositoryAnalyzer("./CDD", ai_analyzer, "databricks")
        comparison_engine = ComparisonEngine()
        excel_generator = ExcelReportGenerator()
        
        # Analyze repositories
        print("\n📊 Analyzing Repositories...")
        hadoop_analysis = hadoop_analyzer.analyze_repository("app-cdd")
        databricks_analysis = databricks_analyzer.analyze_repository("CDD")
        
        # Compare implementations
        comparison = comparison_engine.compare_repositories(hadoop_analysis, databricks_analysis)
        
        # Generate reports
        print("\n📋 Generating Excel Reports...")
        excel_generator.generate_hadoop_report(hadoop_analysis, "CDD_MIGRATION_ANALYSIS_HADOOP_SOURCE_TARGET_MAPPING.xlsx")
        excel_generator.generate_databricks_report(databricks_analysis, "CDD_MIGRATION_ANALYSIS_DATABRICKS_SOURCE_TARGET_MAPPING.xlsx")
        excel_generator.generate_comparison_report(comparison, "CDD_MIGRATION_ANALYSIS_COMPARISON_REPORT.xlsx")
        
        # Print summary
        print("\n" + "="*60)
        print("📊 ANALYSIS SUMMARY")
        print("="*60)
        
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
        
        print(f"\n✅ Analysis Complete!")
        print(f"📊 Reports generated:")
        print(f"   📄 Hadoop Mapping: CDD_MIGRATION_ANALYSIS_HADOOP_SOURCE_TARGET_MAPPING.xlsx")
        print(f"   📄 Databricks Mapping: CDD_MIGRATION_ANALYSIS_DATABRICKS_SOURCE_TARGET_MAPPING.xlsx")
        print(f"   📄 Comparison Report: CDD_MIGRATION_ANALYSIS_COMPARISON_REPORT.xlsx")
        
        print(f"\n🎯 Next Steps:")
        print(f"   1. Review the Excel reports for detailed field mappings")
        print(f"   2. Focus on the Comparison Report to identify differences")
        print(f"   3. Use the field-level mappings to plan your migration")
        print(f"   4. Address any missing tables or fields identified in the comparison")
        
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        print(f"\n🔧 Troubleshooting Tips:")
        print(f"   1. Make sure you have internet connection for Gemini API")
        print(f"   2. Check that your repository directories contain code files")
        print(f"   3. Ensure you have the required packages installed")
        
        # Print detailed error information for debugging
        import traceback
        print(f"\n🔍 Detailed Error Information:")
        traceback.print_exc()

if __name__ == "__main__":
    main()
