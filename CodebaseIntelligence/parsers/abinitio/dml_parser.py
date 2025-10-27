"""
DML Parser
Parses Ab Initio DML (Data Manipulation Language) schema definitions
"""

import re
from typing import Dict, Any, List, Optional
from loguru import logger


class DMLParser:
    """Parser for Ab Initio DML schema definitions"""

    def parse_dml(self, dml_content: str) -> Optional[Dict[str, Any]]:
        """
        Parse DML content and return structured schema
        """
        if not dml_content or not dml_content.strip():
            return None

        try:
            schema = {
                "fields": [],
                "types": [],
                "includes": [],
                "raw": dml_content,
            }

            # Extract include statements
            schema["includes"] = self._extract_includes(dml_content)

            # Extract type definitions
            schema["types"] = self._extract_types(dml_content)

            # Extract field definitions
            schema["fields"] = self._extract_fields(dml_content)

            return schema

        except Exception as e:
            logger.error(f"Error parsing DML: {e}")
            return None

    def _extract_includes(self, dml: str) -> List[str]:
        """Extract include statements"""
        includes = []
        pattern = r'include\s+"([^"]+)"'
        for match in re.finditer(pattern, dml):
            includes.append(match.group(1))
        return includes

    def _extract_types(self, dml: str) -> List[Dict[str, Any]]:
        """Extract user-defined type definitions"""
        types = []

        # Pattern: type TypeName = ...
        type_pattern = r'type\s+(\w+)\s*=\s*(record|vector|union)\s*(.*?)(?=\ntype|\nend|$)'

        for match in re.finditer(type_pattern, dml, re.DOTALL):
            type_name = match.group(1)
            type_kind = match.group(2)
            type_body = match.group(3)

            types.append(
                {
                    "name": type_name,
                    "kind": type_kind,
                    "definition": type_body.strip(),
                }
            )

        return types

    def _extract_fields(self, dml: str) -> List[Dict[str, Any]]:
        """Extract field definitions from DML"""
        fields = []

        # Common DML field patterns:
        # decimal("\x01") fieldname ;
        # string(10) fieldname ;
        # integer fieldname ;
        # date fieldname ;
        # TypeName fieldname ;

        field_patterns = [
            # Pattern: decimal("\x01") fieldname
            r'decimal\s*\("([^"]+)"\)\s+(\w+)\s*;',
            # Pattern: string(length) fieldname or string fieldname
            r'string(?:\((\d+)\))?\s+(\w+)\s*;',
            # Pattern: integer fieldname
            r'integer\s+(\w+)\s*;',
            # Pattern: date fieldname
            r'date\s+(\w+)\s*;',
            # Pattern: datetime fieldname
            r'datetime\s+(\w+)\s*;',
            # Pattern: real fieldname
            r'real\s+(\w+)\s*;',
            # Pattern: TypeName fieldname
            r'(\w+)\s+(\w+)\s*;',
        ]

        # Try decimal pattern
        for match in re.finditer(field_patterns[0], dml):
            separator, field_name = match.groups()
            fields.append(
                {
                    "name": field_name,
                    "type": "decimal",
                    "separator": separator,
                    "nullable": self._check_nullable(dml, field_name),
                }
            )

        # Try string pattern
        for match in re.finditer(field_patterns[1], dml):
            length, field_name = match.groups()
            if length:
                length = int(length)
            fields.append(
                {
                    "name": field_name,
                    "type": "string",
                    "length": length,
                    "nullable": self._check_nullable(dml, field_name),
                }
            )

        # Try simple types
        for pattern, field_type in [
            (field_patterns[2], "integer"),
            (field_patterns[3], "date"),
            (field_patterns[4], "datetime"),
            (field_patterns[5], "real"),
        ]:
            for match in re.finditer(pattern, dml):
                field_name = match.group(1)
                # Skip if already added
                if not any(f["name"] == field_name for f in fields):
                    fields.append(
                        {
                            "name": field_name,
                            "type": field_type,
                            "nullable": self._check_nullable(dml, field_name),
                        }
                    )

        # If no fields found, try generic pattern
        if not fields:
            fields = self._extract_generic_fields(dml)

        return fields

    def _extract_generic_fields(self, dml: str) -> List[Dict[str, Any]]:
        """Extract fields using generic pattern"""
        fields = []

        # Look for lines with field definitions
        lines = dml.split("\n")
        for line in lines:
            line = line.strip()
            if not line or line.startswith("//") or line.startswith("/*"):
                continue

            # Pattern: type name;
            match = re.match(r'(\w+)\s+(\w+)\s*;', line)
            if match:
                field_type, field_name = match.groups()
                # Skip keywords
                if field_type.lower() in ["record", "end", "type", "include"]:
                    continue

                fields.append(
                    {
                        "name": field_name,
                        "type": field_type,
                        "nullable": True,
                    }
                )

        return fields

    def _check_nullable(self, dml: str, field_name: str) -> bool:
        """Check if a field is nullable"""
        # Look for NULL or NOT NULL annotations
        pattern = rf'{field_name}.*?(NOT\s+NULL|NULL)'
        match = re.search(pattern, dml, re.IGNORECASE)
        if match:
            return "NOT NULL" not in match.group(1).upper()
        return True  # Default to nullable

    def extract_field_names(self, dml: str) -> List[str]:
        """Quick extraction of field names only"""
        schema = self.parse_dml(dml)
        if schema and schema.get("fields"):
            return [f["name"] for f in schema["fields"]]
        return []

    def get_field_by_name(self, dml: str, field_name: str) -> Optional[Dict[str, Any]]:
        """Get specific field definition"""
        schema = self.parse_dml(dml)
        if schema and schema.get("fields"):
            for field in schema["fields"]:
                if field["name"] == field_name:
                    return field
        return None
