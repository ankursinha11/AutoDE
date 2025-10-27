"""
Hive Parser
Parses HiveQL/SQL scripts for DDL and DML
"""

import re
from typing import Dict, Any, Optional, List
from pathlib import Path
from loguru import logger

from core.models import Component, Process, ComponentType


class HiveParser:
    """Parser for Hive SQL scripts"""

    def parse_file(self, file_path: str, process: Optional[Process]) -> Optional[Component]:
        """Parse a Hive SQL file"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            file_name = Path(file_path).stem

            # Determine if DDL or DML
            is_ddl = self._is_ddl(content)

            # Extract information
            tables_read = self._extract_tables_read(content)
            tables_written = self._extract_tables_written(content)
            schema = self._extract_schema(content) if is_ddl else None

            comp_id = f"hadoop_hive_{file_name}"
            if process:
                comp_id = f"{process.id}_hive_{file_name}"

            return Component(
                id=comp_id,
                name=file_name,
                component_type=ComponentType.HIVE_QUERY,
                system="hadoop",
                file_path=file_path,
                process_id=process.id if process else None,
                process_name=process.name if process else None,
                tables_read=tables_read,
                tables_written=tables_written,
                output_schema=schema,
                source_code=content,
                code_snippet=content[:1000],
                business_description=f"Hive {'DDL' if is_ddl else 'query'}: {file_name}",
                tags=["ddl"] if is_ddl else ["dml"],
            )

        except Exception as e:
            logger.error(f"Error parsing Hive file {file_path}: {e}")
            return None

    def _is_ddl(self, content: str) -> bool:
        """Check if content is DDL"""
        ddl_keywords = ["CREATE TABLE", "ALTER TABLE", "DROP TABLE"]
        return any(kw in content.upper() for kw in ddl_keywords)

    def _extract_tables_read(self, content: str) -> List[str]:
        """Extract tables being read"""
        tables = []

        patterns = [
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

        patterns = [
            r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)',
            r'INSERT\s+(?:OVERWRITE|INTO)\s+TABLE\s+(\w+)',
            r'ALTER\s+TABLE\s+(\w+)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.extend(matches)

        return list(set(tables))

    def _extract_schema(self, content: str) -> Optional[Dict[str, Any]]:
        """Extract table schema from CREATE TABLE statement"""
        # Pattern: CREATE TABLE ... (col1 type1, col2 type2, ...)
        pattern = r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*?)\)'

        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
        if match:
            table_name, columns_def = match.groups()

            columns = []
            # Parse column definitions
            for col_def in columns_def.split(","):
                col_def = col_def.strip()
                parts = col_def.split()
                if len(parts) >= 2:
                    col_name = parts[0]
                    col_type = parts[1]
                    columns.append({"name": col_name, "type": col_type})

            return {"table": table_name, "columns": columns}

        return None
