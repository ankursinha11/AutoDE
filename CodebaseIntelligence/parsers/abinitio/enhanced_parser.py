"""
Enhanced Ab Initio Parser
Uses FAWN's techniques for more robust parsing:
- Bracket-based block extraction
- YAML pattern configuration
- Subgraph hierarchy tracking
- Better component type identification
"""

import re
import os
import yaml
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from loguru import logger

from core.models import Component, Process, ComponentType, ProcessType, SystemType, DataFlow


class EnhancedAbInitioParser:
    """Enhanced Ab Initio parser using FAWN techniques"""

    def __init__(self):
        self.patterns = self._load_patterns()
        self.processes: List[Process] = []
        self.components: List[Component] = []
        self.data_flows: List[DataFlow] = []
        self.subgraph_hierarchy = ""
        self.prev_subgraph = ""

    def _load_patterns(self) -> Dict[str, Any]:
        """Load patterns from YAML file"""
        pattern_file = Path(__file__).parent / "patterns.yaml"

        if not pattern_file.exists():
            logger.warning(f"Pattern file not found: {pattern_file}, using defaults")
            return self._get_default_patterns()

        try:
            with open(pattern_file, "r") as f:
                patterns = yaml.safe_load(f)
            logger.info("Loaded patterns from YAML")
            return patterns
        except Exception as e:
            logger.error(f"Error loading patterns: {e}, using defaults")
            return self._get_default_patterns()

    def _get_default_patterns(self) -> Dict[str, Any]:
        """Default patterns if YAML not available"""
        return {
            "GRAPH_ID": "0",
            "GRAPH_TYPE": "XXGrepository",
            "ALLOWED_COMPONENT_TYPES": ["Input_File", "Output_File", "Lookup_File"],
            "PROTOTYPE_PATH_PARAM": "!prototype_path",
        }

    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Parse .mp file using enhanced methods"""
        logger.info(f"Parsing with enhanced parser: {file_path}")

        # Read file
        content = self._read_mp_file(file_path)

        # Extract blocks using FAWN's bracket matching
        blocks = self._extract_blocks_bracket_matching(content)
        logger.info(f"Extracted {len(blocks)} blocks")

        # Break down blocks into components
        block_data, blocks_by_graph_id = self._break_block_content(blocks)

        # Parse blocks into components
        parsed_components = self._parse_blocks(blocks_by_graph_id)

        # Create process
        process = self._create_process(file_path, blocks_by_graph_id)

        # Create component objects
        components = []
        for parsed_comp in parsed_components:
            comp = self._create_component(parsed_comp, process)
            if comp:
                components.append(comp)

        # Extract data flows
        flows = self._extract_data_flows(components)

        # Store results
        self.processes.append(process)
        self.components.extend(components)
        self.data_flows.extend(flows)

        process.component_ids = [c.id for c in components]
        process.component_count = len(components)

        return {
            "process": process,
            "components": components,
            "flows": flows,
        }

    def _read_mp_file(self, file_path: str) -> str:
        """Read .mp file content"""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return ""

    def _extract_blocks_bracket_matching(self, content: str) -> List[str]:
        """
        Extract blocks using bracket matching (FAWN's approach)
        More reliable than regex for nested structures
        """
        blocks = []
        stack = []
        current_block = ""

        for char in content:
            if char == "{":
                if not stack:
                    current_block = ""
                stack.append(char)
                current_block += char
            elif char == "}":
                if stack:
                    stack.pop()
                current_block += char
                if not stack:
                    # Complete block found
                    blocks.append(current_block.strip())
            elif stack:
                current_block += char

        if stack:
            logger.warning(f"Unmatched brackets detected: {len(stack)} unmatched '{{' ")

        return blocks

    def _break_down_block(self, block: str) -> Tuple[str, str, str]:
        """Break block into start, internal, end sections"""
        # Look for internal pattern
        internal_pattern = self.patterns.get("BLOCK_INTERNAL_PATTERN", r'\{[^{}]*\{[^{}]*\}[^{}]*\}')
        internal_match = re.search(internal_pattern, block, re.DOTALL)

        if internal_match:
            start_index = internal_match.start()

            # Find matching closing bracket
            stack = []
            end_index = -1
            for i in range(start_index, len(block)):
                if block[i] == "{":
                    stack.append("{")
                elif block[i] == "}":
                    if stack:
                        stack.pop()
                    if not stack:
                        end_index = i + 1
                        break

            if end_index != -1:
                return (
                    block[:start_index].strip(),
                    block[start_index:end_index].strip(),
                    block[end_index:].strip()
                )

        return block, "", ""

    def _break_block_content(self, blocks: List[str]) -> Tuple[List[Dict], Dict[str, List[Dict]]]:
        """
        Break blocks and track subgraph hierarchy (FAWN approach)
        """
        block_with_internal = []
        blocks_by_graph_id = {}
        graph_id_counters = {}
        self.subgraph_hierarchy = ""
        self.prev_subgraph = ""

        for block in blocks:
            start, internal, end = self._break_down_block(block)

            # Extract graph ID
            graph_id = ""
            if start:
                parts = start.strip("{}").split("|")
                if parts:
                    graph_id = parts[0]

                    # Track subgraphs
                    if graph_id == self.patterns.get("GRAPH_ID", "0"):
                        if end:
                            end_parts = end.strip("{}").split("|")
                            if len(end_parts) > 8:
                                grh = end_parts[8].strip()
                                exclude_list = self.patterns.get("SUBGRAPH_EXCLUDE", ["@@@1"])

                                if grh not in exclude_list and grh != self.prev_subgraph:
                                    if self.subgraph_hierarchy == "":
                                        self.subgraph_hierarchy = grh
                                    else:
                                        sep = self.patterns.get("SUBGRAPH_SEPARATOR", ".")
                                        self.subgraph_hierarchy += f"{sep}{grh}"
                                    self.prev_subgraph = grh

            if graph_id and graph_id.isdigit():
                if graph_id not in graph_id_counters:
                    graph_id_counters[graph_id] = 1

                block_idx = graph_id_counters[graph_id]
                graph_id_counters[graph_id] += 1

                block_data = {
                    f"block_idx_{block_idx}": block.strip(),
                    "subgraph_hierarchy": self.subgraph_hierarchy,
                    "start_session": start,
                    "internal_session": internal,
                    "end_session": end
                }

                block_with_internal.append(block_data)

                if graph_id not in blocks_by_graph_id:
                    blocks_by_graph_id[graph_id] = []
                blocks_by_graph_id[graph_id].append(block_data)

        return block_with_internal, blocks_by_graph_id

    def _extract_parameters(self, block: str) -> List[Dict[str, str]]:
        """Extract parameters from block"""
        param_pattern = self.patterns.get(
            "PARAMETER_SECTION_PATTERN",
            r'!fparameters\s*\{([^}]*(?:\{[^}]*\}[^}]*)*)\}'
        )

        match = re.search(param_pattern, block, re.DOTALL)
        parameters = []

        if match:
            param_section = match.group(1)
            nested_blocks = self._extract_blocks_bracket_matching(param_section)

            for param_block in nested_blocks:
                param_dict = self._parse_single_parameter(param_block)
                if param_dict:
                    parameters.append(param_dict)

        return parameters

    def _parse_single_parameter(self, param_block: str) -> Optional[Dict[str, str]]:
        """Parse single parameter block"""
        # Simplified parameter parsing
        # Format: {id|type|!name|value|...}
        pattern = r'\{(\d+)\|([^|]*)\|!([^|]+)\|([^|]*)'
        match = re.search(pattern, param_block)

        if match:
            return {
                "parameter_id": match.group(1),
                "parameter_type": match.group(2),
                "parameter_name": match.group(3),
                "parameter_value": match.group(4).strip() if match.group(4) else "",
            }

        return None

    def _parse_blocks(self, blocks_by_graph_id: Dict[str, List[Dict]]) -> List[Dict]:
        """Parse blocks into structured component data"""
        parsed_components = []

        for graph_id, block_list in blocks_by_graph_id.items():
            for block_entry in block_list:
                block_key = next(k for k in block_entry.keys() if k.startswith("block_idx_"))
                block = block_entry[block_key]

                start = block_entry.get("start_session", "")
                internal = block_entry.get("internal_session", "")
                end = block_entry.get("end_session", "")
                subgraph = block_entry.get("subgraph_hierarchy", "")

                # Parse start section
                start_parts = start.strip("{}").split("|")
                comp_graph_id = start_parts[0] if start_parts else ""
                graph_type = start_parts[1] if len(start_parts) > 1 else ""

                # Extract parameters
                parameters = self._extract_parameters(block)

                # Get component type from !prototype_path
                component_type = self._extract_component_type(parameters)

                # Parse end section for component name
                component_name = self._extract_component_name(end)

                parsed_comp = {
                    "graph_id": comp_graph_id,
                    "graph_type": graph_type,
                    "component_type": component_type,
                    "component_name": component_name,
                    "subgraph_hierarchy": subgraph,
                    "parameters": parameters,
                    "block": block,
                    "internal": internal,
                }

                parsed_components.append(parsed_comp)

        return parsed_components

    def _extract_component_type(self, parameters: List[Dict]) -> str:
        """Extract component type from !prototype_path parameter"""
        prototype_param = self.patterns.get("PROTOTYPE_PATH_PARAM", "!prototype_path")

        for param in parameters:
            if prototype_param in param.get("parameter_name", ""):
                value = param.get("parameter_value", "")
                # Extract type from path like: /path/to/Input_File.comp
                if "/" in value:
                    last_part = value.split("/")[-1]
                    comp_type = last_part.split(".")[0]
                    return comp_type

        return "Unknown"

    def _extract_component_name(self, end_section: str) -> str:
        """Extract component name from end section"""
        if not end_section:
            return ""

        parts = end_section.strip("@{}").split("|")

        # Check for "Ab Initio Software" marker
        component_name_marker = self.patterns.get("COMPONENT_NAME", "Ab Initio Software")
        if component_name_marker in parts:
            idx = parts.index(component_name_marker)
            if idx > 0:
                return parts[idx - 1]

        # Default: 8th position
        if len(parts) > 8:
            return parts[8]

        return parts[0] if parts else ""

    def _create_process(self, file_path: str, blocks_by_graph_id: Dict) -> Process:
        """Create Process object"""
        import hashlib

        file_name = Path(file_path).stem
        process_id = hashlib.md5(f"abinitio_enhanced_{file_name}".encode()).hexdigest()[:16]

        return Process(
            id=process_id,
            name=file_name,
            system=SystemType.ABINITIO,
            process_type=ProcessType.GRAPH,
            file_path=file_path,
            description=f"Ab Initio graph (enhanced): {file_name}",
            metadata={"parser": "enhanced", "blocks_count": len(blocks_by_graph_id)},
        )

    def _create_component(self, parsed_comp: Dict, process: Process) -> Optional[Component]:
        """Create Component object from parsed data"""
        import hashlib

        comp_name = parsed_comp.get("component_name", "unknown")
        comp_type_str = parsed_comp.get("component_type", "Unknown")

        # Map to ComponentType
        comp_type = self._map_component_type(comp_type_str)

        # Generate ID
        comp_id = hashlib.md5(f"{process.id}_{comp_name}".encode()).hexdigest()[:16]

        # Extract I/O from parameters
        inputs, outputs = self._extract_io_from_parameters(parsed_comp["parameters"], comp_type)

        # Extract DML
        dml = self._extract_dml_from_parameters(parsed_comp["parameters"])

        return Component(
            id=comp_id,
            name=comp_name,
            component_type=comp_type,
            system="abinitio",
            process_id=process.id,
            process_name=process.name,
            input_datasets=inputs,
            output_datasets=outputs,
            dml_definition=dml,
            parameters={p["parameter_name"]: p["parameter_value"] for p in parsed_comp["parameters"]},
            metadata={
                "subgraph_hierarchy": parsed_comp.get("subgraph_hierarchy", ""),
                "graph_id": parsed_comp.get("graph_id", ""),
            },
        )

    def _map_component_type(self, comp_type_str: str) -> ComponentType:
        """Map string to ComponentType enum"""
        type_map = {
            "Input_File": ComponentType.INPUT_FILE,
            "Output_File": ComponentType.OUTPUT_FILE,
            "Lookup_File": ComponentType.LOOKUP_FILE,
            "Join": ComponentType.JOIN,
            "Filter": ComponentType.FILTER,
            "Sort": ComponentType.SORT,
            "Rollup": ComponentType.AGGREGATE,
            "Reformat": ComponentType.TRANSFORM,
            "Normalize": ComponentType.TRANSFORM,
            "Denormalize": ComponentType.TRANSFORM,
        }
        return type_map.get(comp_type_str, ComponentType.UNKNOWN)

    def _extract_io_from_parameters(self, parameters: List[Dict], comp_type: ComponentType) -> Tuple[List[str], List[str]]:
        """Extract input/output datasets from parameters"""
        inputs = []
        outputs = []

        layout_param = self.patterns.get("LAYOUT_PARAM", "layout")

        for param in parameters:
            if param.get("parameter_name") == layout_param:
                value = param.get("parameter_value", "")
                if comp_type == ComponentType.INPUT_FILE or comp_type == ComponentType.LOOKUP_FILE:
                    inputs.append(value)
                elif comp_type == ComponentType.OUTPUT_FILE:
                    outputs.append(value)

        return inputs, outputs

    def _extract_dml_from_parameters(self, parameters: List[Dict]) -> str:
        """Extract DML from parameters"""
        layout_param = self.patterns.get("LAYOUT_PARAM", "layout")

        for param in parameters:
            if param.get("parameter_name") == layout_param:
                return param.get("parameter_value", "")

        return ""

    def _extract_data_flows(self, components: List[Component]) -> List[DataFlow]:
        """Extract data flows between components"""
        flows = []

        # Build lookup maps
        comp_by_output = {}
        for comp in components:
            for output in comp.output_datasets:
                if output:
                    comp_by_output[output] = comp

        # Match inputs to outputs
        for comp in components:
            for input_ds in comp.input_datasets:
                if input_ds in comp_by_output:
                    source_comp = comp_by_output[input_ds]
                    flow = DataFlow(
                        source_component_id=source_comp.id,
                        target_component_id=comp.id,
                        dataset_name=input_ds,
                        flow_type="data",
                    )
                    flows.append(flow)

        return flows
