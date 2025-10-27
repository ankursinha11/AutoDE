"""
MP File Parser
Parses Ab Initio .mp files which are structured text files containing graph definitions
"""

import re
from typing import Dict, Any, List, Optional
from loguru import logger


class MPFileParser:
    """Parser for Ab Initio .mp files"""

    def parse(self, content: str) -> Dict[str, Any]:
        """
        Parse .mp file content
        Returns structured data similar to FAWN output
        """
        result = {
            "datasets": [],
            "graph_parameters": {},
            "repository_info": {},
            "graph_flow": [],
        }

        try:
            # Extract graph parameters
            result["graph_parameters"] = self._extract_graph_parameters(content)

            # Extract repository information
            result["repository_info"] = self._extract_repository_info(content)

            # Extract datasets/components
            result["datasets"] = self._extract_datasets(content)

            # Extract graph flow connections
            result["graph_flow"] = self._extract_graph_flow(content)

        except Exception as e:
            logger.error(f"Error parsing MP file: {e}")

        return result

    def _extract_graph_parameters(self, content: str) -> Dict[str, Any]:
        """Extract graph parameters"""
        parameters = {}

        # Look for parameter definitions like: parameter_name = "value"
        param_pattern = r'parameter\s+(\w+)\s*=\s*"([^"]*)"'
        for match in re.finditer(param_pattern, content):
            param_name, param_value = match.groups()
            parameters[param_name] = param_value

        # Look for graph-level parameters
        # Pattern: $AI_PARAM_NAME or ${PARAM_NAME}
        env_vars = re.findall(r'\$\{?([A-Z_]+)\}?', content)
        for var in set(env_vars):
            if var not in parameters:
                parameters[var] = f"${{{var}}}"  # Placeholder

        return parameters

    def _extract_repository_info(self, content: str) -> Dict[str, Any]:
        """Extract repository metadata"""
        repo_info = {}

        # Look for XXGrepository
        repo_pattern = r'XXGrepository\s*\{([^}]+)\}'
        match = re.search(repo_pattern, content, re.DOTALL)
        if match:
            repo_block = match.group(1)
            # Extract key-value pairs
            kv_pattern = r'(\w+):\s*"([^"]*)"'
            for kv_match in re.finditer(kv_pattern, repo_block):
                key, value = kv_match.groups()
                repo_info[key] = value

        return repo_info

    def _extract_datasets(self, content: str) -> List[Dict[str, Any]]:
        """Extract component/dataset definitions"""
        datasets = []

        # Look for component definitions
        # Ab Initio components are defined in various ways
        # This is a simplified parser

        # Pattern 1: Input/Output File components
        file_patterns = [
            (r'(\w+)\s*:\s*Input_File\s*\(\s*"([^"]+)"[^)]*\)', "Input_File"),
            (r'(\w+)\s*:\s*Output_File\s*\(\s*"([^"]+)"[^)]*\)', "Output_File"),
            (r'(\w+)\s*:\s*Lookup_File\s*\(\s*"([^"]+)"[^)]*\)', "Lookup_File"),
        ]

        for pattern, comp_type in file_patterns:
            for match in re.finditer(pattern, content):
                comp_name = match.group(1)
                file_path = match.group(2)

                # Extract DML if present
                dml = self._extract_dml_for_component(content, comp_name)

                datasets.append(
                    {
                        "name": comp_name,
                        "type": comp_type,
                        "dataset_names": file_path,
                        "dml": dml,
                        "key_parameter_value": None,
                        "parameters": {},
                    }
                )

        # Pattern 2: Transform components
        transform_pattern = r'(\w+)\s*:\s*(\w*Transform\w*)\s*\('
        for match in re.finditer(transform_pattern, content):
            comp_name = match.group(1)
            comp_type = match.group(2)

            dml = self._extract_dml_for_component(content, comp_name)

            datasets.append(
                {
                    "name": comp_name,
                    "type": comp_type,
                    "dataset_names": "",
                    "dml": dml,
                    "key_parameter_value": None,
                    "parameters": {},
                }
            )

        # Pattern 3: Join components
        join_pattern = r'(\w+)\s*:\s*Join\s*\('
        for match in re.finditer(join_pattern, content):
            comp_name = match.group(1)

            # Try to extract join key
            key_pattern = rf'{comp_name}.*?key:\s*\[(.*?)\]'
            key_match = re.search(key_pattern, content, re.DOTALL)
            join_key = key_match.group(1) if key_match else None

            datasets.append(
                {
                    "name": comp_name,
                    "type": "Join",
                    "dataset_names": "",
                    "dml": self._extract_dml_for_component(content, comp_name),
                    "key_parameter_value": join_key,
                    "parameters": {},
                }
            )

        # If no datasets found, try generic component extraction
        if not datasets:
            datasets = self._extract_generic_components(content)

        return datasets

    def _extract_dml_for_component(self, content: str, comp_name: str) -> str:
        """Extract DML schema definition for a component"""
        # Look for layout or type definitions
        dml_patterns = [
            rf'{comp_name}.*?layout\s*=\s*\[(.*?)\]',
            rf'{comp_name}.*?type\s*=\s*record\s*\n(.*?)end',
            rf'layout\s+{comp_name}\s*\((.*?)\)',
        ]

        for pattern in dml_patterns:
            match = re.search(pattern, content, re.DOTALL)
            if match:
                return match.group(1)[:1000]  # Limit length

        return ""

    def _extract_generic_components(self, content: str) -> List[Dict[str, Any]]:
        """Generic component extraction as fallback"""
        datasets = []

        # Look for any identifiable component-like structures
        # Common keywords in Ab Initio
        component_keywords = [
            "Input_File",
            "Output_File",
            "Lookup_File",
            "Transform",
            "Join",
            "Filter",
            "Aggregate",
            "Sort",
            "Reformat",
            "Normalize",
        ]

        for keyword in component_keywords:
            pattern = rf'(\w+)\s*:\s*{keyword}'
            for match in re.finditer(pattern, content):
                comp_name = match.group(1)
                datasets.append(
                    {
                        "name": comp_name,
                        "type": keyword,
                        "dataset_names": "",
                        "dml": "",
                        "key_parameter_value": None,
                        "parameters": {},
                    }
                )

        return datasets

    def _extract_graph_flow(self, content: str) -> List[Dict[str, Any]]:
        """Extract data flow connections between components"""
        flows = []

        # Pattern: component1 -> component2
        # Or: output of component1 -> component2
        flow_patterns = [
            r'(\w+)\s*->\s*(\w+)',
            r'output\s+of\s+(\w+)\s*->\s*(\w+)',
            r'(\w+)\.out\s*->\s*(\w+)',
        ]

        for pattern in flow_patterns:
            for match in re.finditer(pattern, content):
                source, target = match.groups()
                flows.append(
                    {
                        "source": source,
                        "target": target,
                        "dataset": "",
                        "type": "data",
                    }
                )

        return flows

    def parse_from_file(self, file_path: str) -> Dict[str, Any]:
        """Parse .mp file from path"""
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        return self.parse(content)
