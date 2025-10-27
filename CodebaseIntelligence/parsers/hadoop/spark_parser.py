"""
Spark Parser
Parses PySpark Python files to extract data processing logic
"""

import ast
import re
from typing import Dict, Any, Optional, List
from pathlib import Path
from loguru import logger

from core.models import Component, Process, ComponentType


class SparkParser:
    """Parser for PySpark Python files"""

    def parse_file(self, file_path: str, process: Optional[Process]) -> Optional[Component]:
        """Parse a PySpark Python file"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            file_name = Path(file_path).stem

            # Extract information
            inputs = self._extract_inputs(content)
            outputs = self._extract_outputs(content)
            tables_read = self._extract_tables_read(content)
            tables_written = self._extract_tables_written(content)
            transformations = self._extract_transformations(content)

            # Try to parse as AST for more detailed analysis
            schema = self._extract_schema_from_ast(content)

            comp_id = f"hadoop_spark_{file_name}"
            if process:
                comp_id = f"{process.id}_spark_{file_name}"

            return Component(
                id=comp_id,
                name=file_name,
                component_type=ComponentType.SPARK_JOB,
                system="hadoop",
                file_path=file_path,
                process_id=process.id if process else None,
                process_name=process.name if process else None,
                input_datasets=inputs,
                output_datasets=outputs,
                tables_read=tables_read,
                tables_written=tables_written,
                output_schema=schema,
                source_code=content,
                code_snippet=content[:1000],  # First 1000 chars
                transformation_logic=transformations,
                business_description=self._infer_business_logic(file_name, content),
            )

        except Exception as e:
            logger.error(f"Error parsing Spark file {file_path}: {e}")
            return None

    def _extract_inputs(self, content: str) -> List[str]:
        """Extract input paths/datasets"""
        inputs = []

        # Pattern: spark.read.format(...).load("path")
        # Pattern: spark.read.parquet("path")
        # Pattern: spark.read.table("table_name")
        patterns = [
            r'\.load\(["\']([^"\']+)["\']\)',
            r'\.parquet\(["\']([^"\']+)["\']\)',
            r'\.csv\(["\']([^"\']+)["\']\)',
            r'\.json\(["\']([^"\']+)["\']\)',
            r'\.orc\(["\']([^"\']+)["\']\)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content)
            inputs.extend(matches)

        return list(set(inputs))

    def _extract_outputs(self, content: str) -> List[str]:
        """Extract output paths/datasets"""
        outputs = []

        # Pattern: df.write.format(...).save("path")
        # Pattern: df.write.parquet("path")
        patterns = [
            r'\.save\(["\']([^"\']+)["\']\)',
            r'\.parquet\(["\']([^"\']+)["\']\)',
            r'\.csv\(["\']([^"\']+)["\']\)',
            r'\.json\(["\']([^"\']+)["\']\)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content)
            outputs.extend(matches)

        return list(set(outputs))

    def _extract_tables_read(self, content: str) -> List[str]:
        """Extract tables being read"""
        tables = []

        # Pattern: spark.table("table_name")
        # Pattern: spark.read.table("table_name")
        # Pattern: spark.sql("SELECT * FROM table_name")
        patterns = [
            r'\.table\(["\']([^"\']+)["\']\)',
            r'\.read\.table\(["\']([^"\']+)["\']\)',
            r'FROM\s+(\w+)',
            r'JOIN\s+(\w+)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.extend(matches)

        return list(set(tables))

    def _extract_tables_written(self, content: str) -> List[str]:
        """Extract tables being written"""
        tables = []

        # Pattern: df.write.saveAsTable("table_name")
        # Pattern: INSERT INTO table_name
        patterns = [
            r'\.saveAsTable\(["\']([^"\']+)["\']\)',
            r'INSERT\s+INTO\s+(\w+)',
            r'CREATE\s+TABLE\s+(\w+)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.extend(matches)

        return list(set(tables))

    def _extract_transformations(self, content: str) -> str:
        """Extract transformation logic summary"""
        transformations = []

        # Common Spark transformations
        spark_ops = [
            "filter",
            "select",
            "withColumn",
            "join",
            "groupBy",
            "agg",
            "orderBy",
            "union",
            "distinct",
            "dropDuplicates",
        ]

        for op in spark_ops:
            if f".{op}(" in content:
                transformations.append(op)

        return ", ".join(transformations) if transformations else "Data processing"

    def _extract_schema_from_ast(self, content: str) -> Optional[Dict[str, Any]]:
        """Try to extract schema using AST"""
        try:
            tree = ast.parse(content)

            # Look for schema definitions
            # Pattern: StructType([StructField(...), ...])
            # This is complex, so we'll do basic extraction

            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    if hasattr(node.func, "attr") and node.func.attr == "StructType":
                        # Found a schema definition
                        return {"found_schema": True}

        except Exception:
            pass

        return None

    def _infer_business_logic(self, file_name: str, content: str) -> str:
        """Infer business logic from file name and content"""
        name_lower = file_name.lower()

        # Check for keywords
        if "parse" in name_lower:
            return f"Parses and processes data - {file_name}"
        elif "merge" in name_lower or "join" in name_lower:
            return f"Merges/joins data - {file_name}"
        elif "gmrn" in name_lower:
            return "Global MRN processing and patient matching"
        elif "lead" in name_lower:
            return "Lead generation and discovery"
        elif "reconcile" in name_lower:
            return "Data reconciliation"
        elif "publish" in name_lower:
            return "Data publication"

        return f"Data transformation - {file_name}"
