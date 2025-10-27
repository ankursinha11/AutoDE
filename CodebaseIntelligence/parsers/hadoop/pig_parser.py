"""
Pig Parser
Parses Apache Pig Latin scripts
"""

import re
from typing import Dict, Any, Optional, List
from pathlib import Path
from loguru import logger

from core.models import Component, Process, ComponentType


class PigParser:
    """Parser for Pig Latin scripts"""

    def parse_file(self, file_path: str, process: Optional[Process]) -> Optional[Component]:
        """Parse a Pig script file"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            file_name = Path(file_path).stem

            # Extract information
            inputs = self._extract_inputs(content)
            outputs = self._extract_outputs(content)
            transformations = self._extract_transformations(content)

            comp_id = f"hadoop_pig_{file_name}"
            if process:
                comp_id = f"{process.id}_pig_{file_name}"

            return Component(
                id=comp_id,
                name=file_name,
                component_type=ComponentType.PIG_SCRIPT,
                system="hadoop",
                file_path=file_path,
                process_id=process.id if process else None,
                process_name=process.name if process else None,
                input_datasets=inputs,
                output_datasets=outputs,
                source_code=content,
                code_snippet=content[:1000],
                transformation_logic=transformations,
                business_description=f"Pig script: {file_name}",
            )

        except Exception as e:
            logger.error(f"Error parsing Pig file {file_path}: {e}")
            return None

    def _extract_inputs(self, content: str) -> List[str]:
        """Extract input paths"""
        inputs = []

        # Pattern: data = LOAD 'path' USING ...
        pattern = r'LOAD\s+["\']([^"\']+)["\']'
        matches = re.findall(pattern, content, re.IGNORECASE)
        inputs.extend(matches)

        return list(set(inputs))

    def _extract_outputs(self, content: str) -> List[str]:
        """Extract output paths"""
        outputs = []

        # Pattern: STORE data INTO 'path' USING ...
        pattern = r'STORE\s+\w+\s+INTO\s+["\']([^"\']+)["\']'
        matches = re.findall(pattern, content, re.IGNORECASE)
        outputs.extend(matches)

        return list(set(outputs))

    def _extract_transformations(self, content: str) -> str:
        """Extract transformation operations"""
        operations = []

        pig_ops = ["FILTER", "FOREACH", "GROUP", "JOIN", "UNION", "DISTINCT", "ORDER"]

        for op in pig_ops:
            if re.search(rf'\b{op}\b', content, re.IGNORECASE):
                operations.append(op)

        return ", ".join(operations) if operations else "Pig transformations"
