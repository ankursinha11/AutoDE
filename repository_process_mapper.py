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

class RepositoryAnalyzer:
    """
    Advanced repository analyzer that can compare Hadoop and Azure Databricks repositories
    by analyzing code content, transformations, and business logic
    """
    
    def __init__(self):
        self.hadoop_processes = []
        self.databricks_processes = []
        self.mappings = []
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
            'dedupe': ['dedupe', 'distinct', 'unique', 'duplicate']
        }
    
    def analyze_hadoop_repository(self, hadoop_path):
        """Analyze Hadoop repository structure and content"""
        print(f"Analyzing Hadoop repository: {hadoop_path}")
        
        hadoop_path = Path(hadoop_path)
        
        # Analyze Pig scripts
        pig_files = list(hadoop_path.rglob("*.pig"))
        for pig_file in pig_files:
            process = self.analyze_pig_script(pig_file)
            if process:
                self.hadoop_processes.append(process)
        
        # Analyze PySpark scripts
        py_files = list(hadoop_path.rglob("*.py"))
        for py_file in py_files:
            process = self.analyze_pyspark_script(py_file)
            if process:
                self.hadoop_processes.append(process)
        
        # Analyze SQL files
        sql_files = list(hadoop_path.rglob("*.sql"))
        for sql_file in sql_files:
            process = self.analyze_sql_script(sql_file)
            if process:
                self.hadoop_processes.append(process)
        
        print(f"Found {len(self.hadoop_processes)} Hadoop processes")
    
    def analyze_databricks_repository(self, databricks_path):
        """Analyze Azure Databricks repository structure and content"""
        print(f"Analyzing Databricks repository: {databricks_path}")
        
        databricks_path = Path(databricks_path)
        
        # Analyze Python notebooks
        py_files = list(databricks_path.rglob("*.py"))
        for py_file in py_files:
            process = self.analyze_databricks_notebook(py_file)
            if process:
                self.databricks_processes.append(process)
        
        # Analyze SQL notebooks
        sql_files = list(databricks_path.rglob("*.sql"))
        for sql_file in sql_files:
            process = self.analyze_databricks_sql(sql_file)
            if process:
                self.databricks_processes.append(process)
        
        # Analyze Scala notebooks
        scala_files = list(databricks_path.rglob("*.scala"))
        for scala_file in scala_files:
            process = self.analyze_scala_notebook(scala_file)
            if process:
                self.databricks_processes.append(process)
        
        print(f"Found {len(self.databricks_processes)} Databricks processes")
    
    def analyze_pig_script(self, pig_file):
        """Analyze Pig script content and extract business logic"""
        try:
            with open(pig_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract Pig-specific patterns
            loads = self.extract_pig_loads(content)
            transformations = self.extract_pig_transformations(content)
            stores = self.extract_pig_stores(content)
            functions = self.extract_pig_functions(content)
            
            # Analyze business logic
            business_logic = self.analyze_business_logic(content, pig_file.name)
            
            # Extract data flow
            data_flow = self.extract_pig_data_flow(content)
            
            process = {
                'type': 'Pig Script',
                'name': pig_file.name,
                'path': str(pig_file),
                'relative_path': str(pig_file.relative_to(pig_file.parents[2])),
                'loads': loads,
                'transformations': transformations,
                'stores': stores,
                'functions': functions,
                'business_logic': business_logic,
                'data_flow': data_flow,
                'content_snippet': content[:500] + "..." if len(content) > 500 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing Pig script {pig_file}: {e}")
            return None
    
    def analyze_pyspark_script(self, py_file):
        """Analyze PySpark script content and extract business logic"""
        try:
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract Spark-specific patterns
            inputs = self.extract_spark_inputs(content)
            outputs = self.extract_spark_outputs(content)
            transformations = self.extract_spark_transformations(content)
            functions = self.extract_spark_functions(content)
            
            # Analyze business logic
            business_logic = self.analyze_business_logic(content, py_file.name)
            
            # Extract data flow
            data_flow = self.extract_spark_data_flow(content)
            
            process = {
                'type': 'PySpark Script',
                'name': py_file.name,
                'path': str(py_file),
                'relative_path': str(py_file.relative_to(py_file.parents[2])),
                'inputs': inputs,
                'outputs': outputs,
                'transformations': transformations,
                'functions': functions,
                'business_logic': business_logic,
                'data_flow': data_flow,
                'content_snippet': content[:500] + "..." if len(content) > 500 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing PySpark script {py_file}: {e}")
            return None
    
    def analyze_databricks_notebook(self, py_file):
        """Analyze Databricks Python notebook content"""
        try:
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract Databricks-specific patterns
            inputs = self.extract_databricks_inputs(content)
            outputs = self.extract_databricks_outputs(content)
            transformations = self.extract_databricks_transformations(content)
            functions = self.extract_databricks_functions(content)
            
            # Analyze business logic
            business_logic = self.analyze_business_logic(content, py_file.name)
            
            # Extract data flow
            data_flow = self.extract_databricks_data_flow(content)
            
            process = {
                'type': 'Databricks Notebook',
                'name': py_file.name,
                'path': str(py_file),
                'relative_path': str(py_file.relative_to(py_file.parents[2])),
                'inputs': inputs,
                'outputs': outputs,
                'transformations': transformations,
                'functions': functions,
                'business_logic': business_logic,
                'data_flow': data_flow,
                'content_snippet': content[:500] + "..." if len(content) > 500 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing Databricks notebook {py_file}: {e}")
            return None
    
    def analyze_sql_script(self, sql_file):
        """Analyze SQL script content"""
        try:
            with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Parse SQL content
            parsed = sqlparse.parse(content)
            
            # Extract SQL patterns
            tables = self.extract_sql_tables(content)
            columns = self.extract_sql_columns(content)
            operations = self.extract_sql_operations(content)
            
            # Analyze business logic
            business_logic = self.analyze_business_logic(content, sql_file.name)
            
            process = {
                'type': 'SQL Script',
                'name': sql_file.name,
                'path': str(sql_file),
                'relative_path': str(sql_file.relative_to(sql_file.parents[2])),
                'tables': tables,
                'columns': columns,
                'operations': operations,
                'business_logic': business_logic,
                'content_snippet': content[:500] + "..." if len(content) > 500 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing SQL script {sql_file}: {e}")
            return None
    
    def analyze_databricks_sql(self, sql_file):
        """Analyze Databricks SQL notebook"""
        return self.analyze_sql_script(sql_file)  # Same analysis for SQL
    
    def analyze_scala_notebook(self, scala_file):
        """Analyze Scala notebook content"""
        try:
            with open(scala_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract Scala-specific patterns
            inputs = self.extract_scala_inputs(content)
            outputs = self.extract_scala_outputs(content)
            transformations = self.extract_scala_transformations(content)
            
            # Analyze business logic
            business_logic = self.analyze_business_logic(content, scala_file.name)
            
            process = {
                'type': 'Scala Notebook',
                'name': scala_file.name,
                'path': str(scala_file),
                'relative_path': str(scala_file.relative_to(scala_file.parents[2])),
                'inputs': inputs,
                'outputs': outputs,
                'transformations': transformations,
                'business_logic': business_logic,
                'content_snippet': content[:500] + "..." if len(content) > 500 else content
            }
            
            return process
            
        except Exception as e:
            print(f"Error analyzing Scala notebook {scala_file}: {e}")
            return None
    
    def extract_pig_loads(self, content):
        """Extract LOAD statements from Pig script"""
        loads = []
        pattern = r"(\w+)\s*=\s*LOAD\s+['\"]([^'\"]+)['\"]"
        matches = re.findall(pattern, content, re.IGNORECASE)
        return [{"alias": match[0], "path": match[1]} for match in matches]
    
    def extract_pig_transformations(self, content):
        """Extract transformations from Pig script"""
        transformations = []
        patterns = [
            r"(\w+)\s*=\s*FOREACH\s+(\w+)\s+GENERATE\s+([^;]+)",
            r"(\w+)\s*=\s*FILTER\s+(\w+)\s+BY\s+([^;]+)",
            r"(\w+)\s*=\s*GROUP\s+(\w+)\s+BY\s+([^;]+)",
            r"(\w+)\s*=\s*JOIN\s+(\w+)\s+BY\s+([^;]+)",
            r"(\w+)\s*=\s*UNION\s+(\w+)\s+(\w+)",
            r"(\w+)\s*=\s*DISTINCT\s+(\w+)"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            transformations.extend(matches)
        
        return transformations
    
    def extract_pig_stores(self, content):
        """Extract STORE statements from Pig script"""
        stores = []
        pattern = r"STORE\s+(\w+)\s+INTO\s+['\"]([^'\"]+)['\"]"
        matches = re.findall(pattern, content, re.IGNORECASE)
        return [{"alias": match[0], "path": match[1]} for match in matches]
    
    def extract_pig_functions(self, content):
        """Extract function definitions from Pig script"""
        functions = []
        pattern = r"DEFINE\s+(\w+)"
        matches = re.findall(pattern, content, re.IGNORECASE)
        return matches
    
    def extract_spark_inputs(self, content):
        """Extract input sources from Spark script"""
        inputs = []
        patterns = [
            r"\.load\(['\"]([^'\"]+)['\"]",
            r"\.read\.format\(['\"]([^'\"]+)['\"]",
            r"\.read\.csv\(['\"]([^'\"]+)['\"]",
            r"\.read\.parquet\(['\"]([^'\"]+)['\"]",
            r"\.read\.json\(['\"]([^'\"]+)['\"]",
            r"spark\.read\.table\(['\"]([^'\"]+)['\"]"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            inputs.extend(matches)
        
        return list(set(inputs))
    
    def extract_spark_outputs(self, content):
        """Extract output sources from Spark script"""
        outputs = []
        patterns = [
            r"\.save\(['\"]([^'\"]+)['\"]",
            r"\.write\.format\(['\"]([^'\"]+)['\"]",
            r"\.write\.csv\(['\"]([^'\"]+)['\"]",
            r"\.write\.parquet\(['\"]([^'\"]+)['\"]",
            r"\.write\.json\(['\"]([^'\"]+)['\"]",
            r"\.write\.saveAsTable\(['\"]([^'\"]+)['\"]"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            outputs.extend(matches)
        
        return list(set(outputs))
    
    def extract_spark_transformations(self, content):
        """Extract data transformations from Spark script"""
        transformations = []
        patterns = [
            r"\.select\(([^)]+)\)",
            r"\.filter\(([^)]+)\)",
            r"\.groupBy\(([^)]+)\)",
            r"\.join\(([^)]+)\)",
            r"\.withColumn\(([^)]+)\)",
            r"\.agg\(([^)]+)\)",
            r"\.drop\(([^)]+)\)",
            r"\.fillna\(([^)]+)\)",
            r"\.replace\(([^)]+)\)",
            r"\.distinct\(\)",
            r"\.union\(([^)]+)\)"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            transformations.extend(matches)
        
        return transformations
    
    def extract_spark_functions(self, content):
        """Extract function definitions from Spark script"""
        functions = []
        pattern = r"def\s+(\w+)\s*\("
        matches = re.findall(pattern, content)
        return matches
    
    def extract_databricks_inputs(self, content):
        """Extract input sources from Databricks notebook"""
        return self.extract_spark_inputs(content)  # Same as Spark
    
    def extract_databricks_outputs(self, content):
        """Extract output sources from Databricks notebook"""
        return self.extract_spark_outputs(content)  # Same as Spark
    
    def extract_databricks_transformations(self, content):
        """Extract transformations from Databricks notebook"""
        return self.extract_spark_transformations(content)  # Same as Spark
    
    def extract_databricks_functions(self, content):
        """Extract function definitions from Databricks notebook"""
        return self.extract_spark_functions(content)  # Same as Spark
    
    def extract_scala_inputs(self, content):
        """Extract input sources from Scala notebook"""
        inputs = []
        patterns = [
            r"\.load\(['\"]([^'\"]+)['\"]",
            r"\.read\.format\(['\"]([^'\"]+)['\"]",
            r"\.read\.csv\(['\"]([^'\"]+)['\"]",
            r"\.read\.parquet\(['\"]([^'\"]+)['\"]"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            inputs.extend(matches)
        
        return list(set(inputs))
    
    def extract_scala_outputs(self, content):
        """Extract output sources from Scala notebook"""
        outputs = []
        patterns = [
            r"\.save\(['\"]([^'\"]+)['\"]",
            r"\.write\.format\(['\"]([^'\"]+)['\"]",
            r"\.write\.csv\(['\"]([^'\"]+)['\"]",
            r"\.write\.parquet\(['\"]([^'\"]+)['\"]"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            outputs.extend(matches)
        
        return list(set(outputs))
    
    def extract_scala_transformations(self, content):
        """Extract transformations from Scala notebook"""
        transformations = []
        patterns = [
            r"\.select\(([^)]+)\)",
            r"\.filter\(([^)]+)\)",
            r"\.groupBy\(([^)]+)\)",
            r"\.join\(([^)]+)\)",
            r"\.withColumn\(([^)]+)\)"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            transformations.extend(matches)
        
        return transformations
    
    def extract_sql_tables(self, content):
        """Extract table references from SQL"""
        tables = []
        patterns = [
            r"FROM\s+(\w+)",
            r"JOIN\s+(\w+)",
            r"INTO\s+(\w+)",
            r"UPDATE\s+(\w+)",
            r"DELETE\s+FROM\s+(\w+)"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.extend(matches)
        
        return list(set(tables))
    
    def extract_sql_columns(self, content):
        """Extract column references from SQL"""
        columns = []
        pattern = r"SELECT\s+([^FROM]+)"
        matches = re.findall(pattern, content, re.IGNORECASE)
        for match in matches:
            cols = [col.strip() for col in match.split(',')]
            columns.extend(cols)
        
        return list(set(columns))
    
    def extract_sql_operations(self, content):
        """Extract SQL operations"""
        operations = []
        patterns = [
            r"(SELECT)",
            r"(INSERT)",
            r"(UPDATE)",
            r"(DELETE)",
            r"(CREATE)",
            r"(ALTER)",
            r"(DROP)"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            operations.extend(matches)
        
        return list(set(operations))
    
    def analyze_business_logic(self, content, filename):
        """Analyze business logic from content and filename"""
        logic_hints = []
        content_lower = content.lower()
        filename_lower = filename.lower()
        
        # Analyze filename patterns
        for key, patterns in self.business_logic_patterns.items():
            for pattern in patterns:
                if pattern in filename_lower:
                    logic_hints.append(f"{key}_processing")
                    break
        
        # Analyze content patterns
        for key, patterns in self.business_logic_patterns.items():
            for pattern in patterns:
                if pattern in content_lower:
                    logic_hints.append(f"{key}_logic")
                    break
        
        # Look for specific business terms
        business_terms = {
            'patient': 'patient_data_processing',
            'coverage': 'coverage_discovery',
            'insurance': 'insurance_processing',
            'demographic': 'demographic_processing',
            'lead': 'lead_generation',
            'edi': 'edi_processing',
            'hospital': 'hospital_data_processing',
            'policy': 'policy_processing',
            'permid': 'permid_processing',
            'gmrn': 'gmrn_processing'
        }
        
        for term, description in business_terms.items():
            if term in content_lower:
                logic_hints.append(description)
        
        return list(set(logic_hints))
    
    def extract_pig_data_flow(self, content):
        """Extract data flow from Pig script"""
        flow = []
        # Extract variable assignments and their relationships
        pattern = r"(\w+)\s*=\s*(\w+)\s+([^;]+)"
        matches = re.findall(pattern, content)
        for match in matches:
            flow.append(f"{match[0]} = {match[1]} {match[2]}")
        return flow
    
    def extract_spark_data_flow(self, content):
        """Extract data flow from Spark script"""
        flow = []
        # Extract DataFrame operations
        pattern = r"(\w+)\s*=\s*(\w+)\.[^=]+"
        matches = re.findall(pattern, content)
        for match in matches:
            flow.append(f"{match[0]} = {match[1]}.operation")
        return flow
    
    def extract_databricks_data_flow(self, content):
        """Extract data flow from Databricks notebook"""
        return self.extract_spark_data_flow(content)  # Same as Spark
    
    def create_mappings(self):
        """Create mappings between Hadoop and Databricks processes"""
        print("Creating process mappings...")
        
        for hadoop_process in self.hadoop_processes:
            best_matches = []
            
            for databricks_process in self.databricks_processes:
                similarity_score = self.calculate_similarity(hadoop_process, databricks_process)
                
                if similarity_score > 0.3:  # Threshold for similarity
                    best_matches.append({
                        'databricks_process': databricks_process,
                        'similarity_score': similarity_score,
                        'matching_factors': self.get_matching_factors(hadoop_process, databricks_process)
                    })
            
            # Sort by similarity score
            best_matches.sort(key=lambda x: x['similarity_score'], reverse=True)
            
            if best_matches:
                mapping = {
                    'hadoop_process': hadoop_process,
                    'best_match': best_matches[0],
                    'all_matches': best_matches[:5]  # Top 5 matches
                }
                self.mappings.append(mapping)
        
        print(f"Created {len(self.mappings)} process mappings")
    
    def calculate_similarity(self, hadoop_process, databricks_process):
        """Calculate similarity between Hadoop and Databricks processes"""
        score = 0.0
        
        # Filename similarity
        hadoop_name = hadoop_process['name'].lower()
        databricks_name = databricks_process['name'].lower()
        
        # Check for common keywords
        common_keywords = ['permid', 'policy', 'address', 'consumer', 'phone', 'merge', 'publish', 'validate', 'parse']
        for keyword in common_keywords:
            if keyword in hadoop_name and keyword in databricks_name:
                score += 0.2
        
        # Business logic similarity
        hadoop_logic = set(hadoop_process['business_logic'])
        databricks_logic = set(databricks_process['business_logic'])
        logic_intersection = hadoop_logic.intersection(databricks_logic)
        if hadoop_logic or databricks_logic:
            score += len(logic_intersection) / max(len(hadoop_logic), len(databricks_logic)) * 0.4
        
        # Data flow similarity
        if 'data_flow' in hadoop_process and 'data_flow' in databricks_process:
            hadoop_flow = set(hadoop_process['data_flow'])
            databricks_flow = set(databricks_process['data_flow'])
            flow_intersection = hadoop_flow.intersection(databricks_flow)
            if hadoop_flow or databricks_flow:
                score += len(flow_intersection) / max(len(hadoop_flow), len(databricks_flow)) * 0.2
        
        # Content similarity
        hadoop_content = hadoop_process['content_snippet'].lower()
        databricks_content = databricks_process['content_snippet'].lower()
        
        # Check for common patterns in content
        common_patterns = ['spark', 'dataframe', 'select', 'filter', 'join', 'groupby', 'agg']
        for pattern in common_patterns:
            if pattern in hadoop_content and pattern in databricks_content:
                score += 0.05
        
        return min(score, 1.0)  # Cap at 1.0
    
    def get_matching_factors(self, hadoop_process, databricks_process):
        """Get factors that contribute to the match"""
        factors = []
        
        # Filename similarity
        hadoop_name = hadoop_process['name'].lower()
        databricks_name = databricks_process['name'].lower()
        
        common_keywords = ['permid', 'policy', 'address', 'consumer', 'phone', 'merge', 'publish', 'validate', 'parse']
        matching_keywords = [kw for kw in common_keywords if kw in hadoop_name and kw in databricks_name]
        if matching_keywords:
            factors.append(f"Common keywords: {', '.join(matching_keywords)}")
        
        # Business logic similarity
        hadoop_logic = set(hadoop_process['business_logic'])
        databricks_logic = set(databricks_process['business_logic'])
        common_logic = hadoop_logic.intersection(databricks_logic)
        if common_logic:
            factors.append(f"Common business logic: {', '.join(common_logic)}")
        
        # Process type similarity
        if hadoop_process['type'] == 'PySpark Script' and databricks_process['type'] == 'Databricks Notebook':
            factors.append("Both use Spark/Python")
        elif hadoop_process['type'] == 'SQL Script' and databricks_process['type'] == 'SQL Script':
            factors.append("Both use SQL")
        
        return factors
    
    def create_excel_mapping(self, output_file="REPOSITORY_PROCESS_MAPPING.xlsx"):
        """Create Excel file with process mappings"""
        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        
        # Create Process Mappings sheet
        self.create_process_mappings_sheet(wb)
        
        # Create Hadoop Processes sheet
        self.create_hadoop_processes_sheet(wb)
        
        # Create Databricks Processes sheet
        self.create_databricks_processes_sheet(wb)
        
        # Create Similarity Analysis sheet
        self.create_similarity_analysis_sheet(wb)
        
        wb.save(output_file)
        print(f"Excel mapping file created: {output_file}")
    
    def create_process_mappings_sheet(self, wb):
        """Create Process Mappings sheet"""
        ws = wb.create_sheet("Process_Mappings")
        
        headers = [
            "Hadoop Process", "Hadoop Type", "Hadoop Path", "Hadoop Business Logic",
            "Databricks Process", "Databricks Type", "Databricks Path", "Databricks Business Logic",
            "Similarity Score", "Matching Factors", "Confidence Level"
        ]
        ws.append(headers)
        
        for mapping in self.mappings:
            hadoop_process = mapping['hadoop_process']
            best_match = mapping['best_match']
            databricks_process = best_match['databricks_process']
            
            row = [
                hadoop_process['name'],
                hadoop_process['type'],
                hadoop_process['relative_path'],
                ', '.join(hadoop_process['business_logic']),
                databricks_process['name'],
                databricks_process['type'],
                databricks_process['relative_path'],
                ', '.join(databricks_process['business_logic']),
                f"{best_match['similarity_score']:.2f}",
                '; '.join(best_match['matching_factors']),
                self.get_confidence_level(best_match['similarity_score'])
            ]
            ws.append(row)
        
        self.format_sheet(ws)
    
    def create_hadoop_processes_sheet(self, wb):
        """Create Hadoop Processes sheet"""
        ws = wb.create_sheet("Hadoop_Processes")
        
        headers = [
            "Process Name", "Type", "Path", "Business Logic", "Data Flow", "Content Snippet"
        ]
        ws.append(headers)
        
        for process in self.hadoop_processes:
            row = [
                process['name'],
                process['type'],
                process['relative_path'],
                ', '.join(process['business_logic']),
                '; '.join(process.get('data_flow', [])),
                process['content_snippet']
            ]
            ws.append(row)
        
        self.format_sheet(ws)
    
    def create_databricks_processes_sheet(self, wb):
        """Create Databricks Processes sheet"""
        ws = wb.create_sheet("Databricks_Processes")
        
        headers = [
            "Process Name", "Type", "Path", "Business Logic", "Data Flow", "Content Snippet"
        ]
        ws.append(headers)
        
        for process in self.databricks_processes:
            row = [
                process['name'],
                process['type'],
                process['relative_path'],
                ', '.join(process['business_logic']),
                '; '.join(process.get('data_flow', [])),
                process['content_snippet']
            ]
            ws.append(row)
        
        self.format_sheet(ws)
    
    def create_similarity_analysis_sheet(self, wb):
        """Create Similarity Analysis sheet"""
        ws = wb.create_sheet("Similarity_Analysis")
        
        headers = [
            "Hadoop Process", "Databricks Process", "Similarity Score", "Matching Factors", "Recommendation"
        ]
        ws.append(headers)
        
        for mapping in self.mappings:
            hadoop_process = mapping['hadoop_process']
            
            for match in mapping['all_matches']:
                databricks_process = match['databricks_process']
                
                row = [
                    hadoop_process['name'],
                    databricks_process['name'],
                    f"{match['similarity_score']:.2f}",
                    '; '.join(match['matching_factors']),
                    self.get_recommendation(match['similarity_score'])
                ]
                ws.append(row)
        
        self.format_sheet(ws)
    
    def get_confidence_level(self, score):
        """Get confidence level based on similarity score"""
        if score >= 0.8:
            return "High"
        elif score >= 0.6:
            return "Medium"
        elif score >= 0.4:
            return "Low"
        else:
            return "Very Low"
    
    def get_recommendation(self, score):
        """Get recommendation based on similarity score"""
        if score >= 0.8:
            return "Strong Match - Likely Equivalent"
        elif score >= 0.6:
            return "Good Match - Probably Equivalent"
        elif score >= 0.4:
            return "Possible Match - Review Required"
        else:
            return "Weak Match - Manual Review"
    
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
    """Main function to run the repository analyzer"""
    analyzer = RepositoryAnalyzer()
    
    # Get repository paths from user
    print("=== Repository Process Mapping Tool ===")
    print("This tool analyzes Hadoop and Azure Databricks repositories to find equivalent processes.")
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
    analyzer.analyze_hadoop_repository(hadoop_path)
    analyzer.analyze_databricks_repository(databricks_path)
    
    # Create mappings
    analyzer.create_mappings()
    
    # Create Excel output
    output_file = input("Enter output Excel filename (default: REPOSITORY_PROCESS_MAPPING.xlsx): ").strip()
    if not output_file:
        output_file = "REPOSITORY_PROCESS_MAPPING.xlsx"
    
    analyzer.create_excel_mapping(output_file)
    
    print(f"\nAnalysis complete!")
    print(f"Found {len(analyzer.hadoop_processes)} Hadoop processes")
    print(f"Found {len(analyzer.databricks_processes)} Databricks processes")
    print(f"Created {len(analyzer.mappings)} process mappings")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    main()
