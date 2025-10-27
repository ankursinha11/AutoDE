"""
Databricks Notebook Parser
Parses .py, .sql, and .scala notebooks to extract data transformations
"""

import re
import json
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from loguru import logger

from core.models import Component, ComponentType, DataType, TransformationRule


class NotebookParser:
    """Parser for Databricks notebooks (.py, .sql, .scala)"""

    def __init__(self):
        # SQL table/view patterns
        self.sql_table_patterns = [
            r"FROM\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)",
            r"JOIN\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)",
            r"INTO\s+(?:TABLE\s+)?([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)",
            r"TABLE\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)",
        ]

        # PySpark DataFrame patterns
        self.pyspark_read_patterns = [
            r"spark\.read\.(?:format|table|parquet|csv|json|delta)\(['\"]([^'\"]+)['\"]",
            r"spark\.table\(['\"]([^'\"]+)['\"]",
            r"\.load\(['\"]([^'\"]+)['\"]",
        ]

        self.pyspark_write_patterns = [
            r"\.write\.(?:format|saveAsTable|parquet|csv|json|delta)\(['\"]([^'\"]+)['\"]",
            r"\.saveAsTable\(['\"]([^'\"]+)['\"]",
            r"\.save\(['\"]([^'\"]+)['\"]",
        ]

        # Transformation patterns
        self.transform_patterns = {
            "filter": r"\.(?:filter|where)\(",
            "select": r"\.select\(",
            "groupBy": r"\.groupBy\(",
            "join": r"\.join\(",
            "agg": r"\.agg\(",
            "withColumn": r"\.withColumn\(",
            "drop": r"\.drop\(",
            "distinct": r"\.distinct\(",
        }

    def parse_notebook(self, notebook_path: str) -> List[Component]:
        """Parse a single notebook file"""
        notebook_path = Path(notebook_path)

        if not notebook_path.exists():
            logger.warning(f"Notebook not found: {notebook_path}")
            return []

        logger.debug(f"Parsing notebook: {notebook_path.name}")

        # Determine notebook type
        if notebook_path.suffix == ".py":
            return self._parse_python_notebook(notebook_path)
        elif notebook_path.suffix == ".sql":
            return self._parse_sql_notebook(notebook_path)
        elif notebook_path.suffix == ".scala":
            return self._parse_scala_notebook(notebook_path)
        elif notebook_path.suffix == ".ipynb":
            return self._parse_jupyter_notebook(notebook_path)
        else:
            logger.warning(f"Unsupported notebook format: {notebook_path.suffix}")
            return []

    def _parse_python_notebook(self, notebook_path: Path) -> List[Component]:
        """Parse Python/PySpark notebook"""
        try:
            with open(notebook_path, "r", encoding="utf-8") as f:
                content = f.read()

            components = []

            # Extract inputs (reads)
            inputs = self._extract_pyspark_reads(content)

            # Extract outputs (writes)
            outputs = self._extract_pyspark_writes(content)

            # Extract transformations
            transformations = self._extract_pyspark_transformations(content)

            # Extract business logic from comments/docstrings
            business_logic = self._extract_business_logic(content)

            # Create component
            component = Component(
                id=f"databricks_{notebook_path.stem}",
                name=notebook_path.stem,
                component_type=ComponentType.SPARK_JOB,
                system="databricks",
                file_path=str(notebook_path),
                input_datasets=list(inputs),
                output_datasets=list(outputs),
                transformation_rules=transformations,
                business_logic=business_logic,
                metadata={
                    "language": "python",
                    "notebook_type": "pyspark",
                    "file_name": notebook_path.name,
                },
            )

            components.append(component)
            return components

        except Exception as e:
            logger.error(f"Error parsing Python notebook {notebook_path}: {e}")
            return []

    def _parse_sql_notebook(self, notebook_path: Path) -> List[Component]:
        """Parse SQL notebook"""
        try:
            with open(notebook_path, "r", encoding="utf-8") as f:
                content = f.read()

            components = []

            # Split by SQL statements (simplified - could use sqlparse for better accuracy)
            statements = self._split_sql_statements(content)

            for idx, statement in enumerate(statements):
                # Extract inputs (FROM, JOIN)
                inputs = self._extract_sql_tables(statement, ["FROM", "JOIN"])

                # Extract outputs (INTO, CREATE TABLE)
                outputs = self._extract_sql_tables(statement, ["INTO", "CREATE TABLE", "INSERT INTO"])

                # Determine transformation type
                transform_type = self._identify_sql_transform_type(statement)

                transformations = [
                    TransformationRule(
                        rule_type=transform_type,
                        expression=statement[:200],  # First 200 chars
                        description=f"SQL {transform_type}",
                    )
                ]

                component = Component(
                    id=f"databricks_{notebook_path.stem}_stmt{idx}",
                    name=f"{notebook_path.stem}_statement_{idx}",
                    component_type=ComponentType.SQL_QUERY,
                    system="databricks",
                    file_path=str(notebook_path),
                    input_datasets=list(inputs),
                    output_datasets=list(outputs),
                    transformation_rules=transformations,
                    metadata={
                        "language": "sql",
                        "statement_index": idx,
                        "file_name": notebook_path.name,
                    },
                )

                components.append(component)

            return components

        except Exception as e:
            logger.error(f"Error parsing SQL notebook {notebook_path}: {e}")
            return []

    def _parse_scala_notebook(self, notebook_path: Path) -> List[Component]:
        """Parse Scala/Spark notebook"""
        try:
            with open(notebook_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Similar to Python but with Scala syntax
            # For now, use similar patterns (Spark API is similar)
            inputs = self._extract_pyspark_reads(content)  # Similar patterns work
            outputs = self._extract_pyspark_writes(content)
            transformations = self._extract_pyspark_transformations(content)

            component = Component(
                id=f"databricks_{notebook_path.stem}",
                name=notebook_path.stem,
                component_type=ComponentType.SPARK_JOB,
                system="databricks",
                file_path=str(notebook_path),
                input_datasets=list(inputs),
                output_datasets=list(outputs),
                transformation_rules=transformations,
                metadata={
                    "language": "scala",
                    "notebook_type": "spark_scala",
                    "file_name": notebook_path.name,
                },
            )

            return [component]

        except Exception as e:
            logger.error(f"Error parsing Scala notebook {notebook_path}: {e}")
            return []

    def _parse_jupyter_notebook(self, notebook_path: Path) -> List[Component]:
        """Parse Jupyter notebook (.ipynb)"""
        try:
            with open(notebook_path, "r", encoding="utf-8") as f:
                nb = json.load(f)

            # Extract all code cells
            code_cells = [
                cell["source"]
                for cell in nb.get("cells", [])
                if cell.get("cell_type") == "code"
            ]

            # Combine all code
            content = "\n".join(
                ["".join(cell) if isinstance(cell, list) else cell for cell in code_cells]
            )

            # Parse like Python notebook
            inputs = self._extract_pyspark_reads(content)
            outputs = self._extract_pyspark_writes(content)
            transformations = self._extract_pyspark_transformations(content)

            component = Component(
                id=f"databricks_{notebook_path.stem}",
                name=notebook_path.stem,
                component_type=ComponentType.SPARK_JOB,
                system="databricks",
                file_path=str(notebook_path),
                input_datasets=list(inputs),
                output_datasets=list(outputs),
                transformation_rules=transformations,
                metadata={
                    "language": "python",
                    "notebook_type": "jupyter",
                    "file_name": notebook_path.name,
                },
            )

            return [component]

        except Exception as e:
            logger.error(f"Error parsing Jupyter notebook {notebook_path}: {e}")
            return []

    def _extract_pyspark_reads(self, content: str) -> Set[str]:
        """Extract input datasets from PySpark code"""
        inputs = set()

        for pattern in self.pyspark_read_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            inputs.update(matches)

        return inputs

    def _extract_pyspark_writes(self, content: str) -> Set[str]:
        """Extract output datasets from PySpark code"""
        outputs = set()

        for pattern in self.pyspark_write_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            outputs.update(matches)

        return outputs

    def _extract_pyspark_transformations(self, content: str) -> List[TransformationRule]:
        """Extract transformation operations"""
        transformations = []

        for transform_type, pattern in self.transform_patterns.items():
            matches = re.finditer(pattern, content)
            for match in matches:
                # Extract the transformation expression (next 100 chars after match)
                start_pos = match.start()
                end_pos = min(start_pos + 150, len(content))
                expression = content[start_pos:end_pos].strip()

                transformations.append(
                    TransformationRule(
                        rule_type=transform_type,
                        expression=expression,
                        description=f"PySpark {transform_type} operation",
                    )
                )

        return transformations

    def _extract_sql_tables(self, sql: str, keywords: List[str]) -> Set[str]:
        """Extract table names from SQL for given keywords"""
        tables = set()

        for keyword in keywords:
            pattern = rf"{keyword}\s+(?:TABLE\s+)?([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)"
            matches = re.findall(pattern, sql, re.IGNORECASE)
            tables.update(matches)

        return tables

    def _split_sql_statements(self, content: str) -> List[str]:
        """Split SQL content into individual statements"""
        # Remove comments
        content = re.sub(r"--.*$", "", content, flags=re.MULTILINE)
        content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)

        # Split by semicolon
        statements = [s.strip() for s in content.split(";") if s.strip()]

        return statements

    def _identify_sql_transform_type(self, sql: str) -> str:
        """Identify the type of SQL transformation"""
        sql_upper = sql.upper()

        if "CREATE" in sql_upper and "TABLE" in sql_upper:
            return "create_table"
        elif "INSERT" in sql_upper:
            return "insert"
        elif "UPDATE" in sql_upper:
            return "update"
        elif "DELETE" in sql_upper:
            return "delete"
        elif "MERGE" in sql_upper:
            return "merge"
        elif "SELECT" in sql_upper:
            return "select"
        else:
            return "unknown"

    def _extract_business_logic(self, content: str) -> Optional[str]:
        """Extract business logic from comments and docstrings"""
        # Look for docstrings
        docstring_pattern = r'"""(.*?)"""'
        docstrings = re.findall(docstring_pattern, content, re.DOTALL)

        if docstrings:
            # Return first non-empty docstring
            for doc in docstrings:
                doc = doc.strip()
                if len(doc) > 20:  # Meaningful docstring
                    return doc[:500]  # First 500 chars

        # Look for comment blocks
        comment_pattern = r"#\s*(.*?)(?:\n|$)"
        comments = re.findall(comment_pattern, content)

        if comments:
            meaningful_comments = [c.strip() for c in comments if len(c.strip()) > 20]
            if meaningful_comments:
                return " ".join(meaningful_comments[:5])  # First 5 comments

        return None
