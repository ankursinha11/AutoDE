"""
VM_FAWN Excel to Enhanced JSON Converter
=========================================

Reads VM_FAWN generated Excel files and converts to Enhanced Parser JSON format.

Usage:
    1. Run VM_FAWN parser manually on .mp file to generate Excel
    2. Use this converter to create JSON that STAG/GraphFlow can use
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import re


class VMFAWNExcelConverter:
    """Converts VM_FAWN Excel output to Enhanced Parser JSON format"""

    def convert_excel_to_json(
        self,
        vm_fawn_excel_path: str,
        source_mp_file_path: str,
        output_json_path: str = None
    ) -> Dict[str, Any]:
        """
        Convert VM_FAWN Excel to Enhanced Parser JSON format

        Args:
            vm_fawn_excel_path: Path to VM_FAWN generated Excel file
            source_mp_file_path: Path to original .mp file (for raw content)
            output_json_path: Optional output path for JSON file

        Returns:
            Dictionary in Enhanced Parser format with VM_FAWN data
        """
        vm_fawn_excel = Path(vm_fawn_excel_path)
        source_mp = Path(source_mp_file_path)

        if not vm_fawn_excel.exists():
            raise FileNotFoundError(f"VM_FAWN Excel not found: {vm_fawn_excel}")
        if not source_mp.exists():
            raise FileNotFoundError(f"Source .mp file not found: {source_mp}")

        print(f"🔄 Converting VM_FAWN Excel to Enhanced JSON")
        print(f"   Input Excel: {vm_fawn_excel.name}")
        print(f"   Source MP: {source_mp.name}")

        # Read Excel sheets
        excel_data = pd.ExcelFile(vm_fawn_excel)
        print(f"   Sheets found: {excel_data.sheet_names}")

        # Read each sheet
        sheets = {}
        for sheet_name in ['DataSet', 'Component&Fields', 'GraphParameters', 'GraphFlow']:
            if sheet_name in excel_data.sheet_names:
                sheets[sheet_name] = pd.read_excel(vm_fawn_excel, sheet_name=sheet_name)
                print(f"   ✓ {sheet_name}: {len(sheets[sheet_name])} rows")
            else:
                sheets[sheet_name] = pd.DataFrame()
                print(f"   ⚠ {sheet_name}: Not found")

        # Read raw .mp content
        with open(source_mp, 'r', encoding='latin-1') as f:
            raw_content = f.read()

        # Convert to Enhanced Parser format
        result = self._build_enhanced_format(sheets, source_mp, raw_content)

        # Save to JSON if output path provided
        if output_json_path:
            output_path = Path(output_json_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
            print(f"   ✅ Saved to: {output_path}")

        return result

    def _build_enhanced_format(
        self,
        sheets: Dict[str, pd.DataFrame],
        source_mp: Path,
        raw_content: str
    ) -> Dict[str, Any]:
        """Build Enhanced Parser format from VM_FAWN sheets"""

        component_df = sheets.get('Component&Fields', pd.DataFrame())
        dataset_df = sheets.get('DataSet', pd.DataFrame())
        graph_param_df = sheets.get('GraphParameters', pd.DataFrame())

        vertices = {}
        flows = {}
        ports = {}
        graphs = {}

        # Build vertices from Component&Fields (transformations)
        transform_count = 0
        for idx, row in component_df.iterrows():
            vertex_id = f"transform_{idx}"
            comp_name = self._safe_str(row.get('Component Name', f'transform_{idx}'))
            transform_comp = self._safe_str(row.get('Transform Component', ''))
            transform_rule = self._safe_str(row.get('Transform Rule', ''))
            trans_location = self._safe_str(row.get('TransLocation', ''))
            keys = self._safe_str(row.get('Keys', ''))

            # Extract DML/transformation type from transform_rule
            dml_match = re.search(r'dml\s*[:=]\s*["\']?([^"\';\s]+)', transform_rule, re.IGNORECASE)
            dml = dml_match.group(1) if dml_match else ''

            has_transform = bool(transform_rule and transform_rule != 'nan' and len(transform_rule.strip()) > 0)
            if has_transform:
                transform_count += 1

            vertices[vertex_id] = {
                'component_id': str(idx),
                'component_type': transform_comp if transform_comp else 'Transform',
                'name': comp_name,
                'type': transform_comp,
                'attributes': {
                    'transform_component': transform_comp,
                    'transform_rule': transform_rule[:500] if transform_rule else '',  # Truncate long rules
                    'trans_location': trans_location,
                    'keys': keys,
                    'dml': dml,
                    'layout': trans_location,
                    'subgraph_hierarchy': self._safe_str(row.get('subgraph_hierarchy', '')),
                    'final_subgraph': self._safe_str(row.get('final_subgraph', '')),
                    'component_phase': self._safe_str(row.get('component_phase', '')),
                    'sr_no': self._safe_str(row.get('sr_no', ''))
                },
                'raw_content': f"Transform: {transform_comp}, Rule: {transform_rule[:200]}"
            }

        # Build vertices from DataSet (input/output files)
        for idx, row in dataset_df.iterrows():
            vertex_id = f"dataset_{idx}"
            comp_label = self._safe_str(row.get('CompDisplay Label', f'dataset_{idx}'))
            comp_type = self._safe_str(row.get('Component Type', 'Dataset'))
            dataset_names = self._safe_str(row.get('DataSet Names', ''))
            dml = self._safe_str(row.get('DML', ''))
            key_param = self._safe_str(row.get('Key Parameter Value', ''))

            vertices[vertex_id] = {
                'component_id': f"ds_{idx}",
                'component_type': comp_type,
                'name': comp_label,
                'type': comp_type,
                'attributes': {
                    'layout': dataset_names,
                    'dml': dml,
                    'key_parameter': key_param,
                    'dataset_type': comp_type,
                    'dataset_names': dataset_names
                },
                'raw_content': f"Dataset: {comp_label}, Type: {comp_type}, DML: {dml[:100]}"
            }

        # Build flows from GraphFlow sheet if available
        graphflow_df = sheets.get('GraphFlow', pd.DataFrame())
        flow_count_from_sheet = 0
        for idx, row in graphflow_df.iterrows():
            flow_id = f"flow_{idx}"
            from_comp = self._safe_str(row.get('From Component', ''))
            to_comp = self._safe_str(row.get('To Component', ''))
            from_port = self._safe_str(row.get('From Port', 'out'))
            to_port = self._safe_str(row.get('To Port', 'in'))

            if from_comp and to_comp:
                flows[flow_id] = {
                    'component_id': str(idx),
                    'component_type': 'XXGflow',
                    'name': f"flow_{from_comp}_to_{to_comp}",
                    'type': 'DATA_FLOW',
                    'from_vertex': from_comp,
                    'to_vertex': to_comp,
                    'from_port': from_port,
                    'to_port': to_port,
                    'flow_type': 'DATA_FLOW',
                    'raw_content': f"Flow: {from_comp}:{from_port} → {to_comp}:{to_port}"
                }
                flow_count_from_sheet += 1

        # Fallback: Extract flows from raw content if GraphFlow sheet is empty
        if flow_count_from_sheet == 0:
            print(f"   ⚠️ GraphFlow sheet empty, extracting flows from raw content...")
            extracted_flows = self._extract_flows_from_raw(raw_content, vertices)
            flows.update(extracted_flows)
            print(f"   ✓ Extracted {len(extracted_flows)} flows from raw content")

        # Build ports from flows and vertices
        ports = self._build_ports_from_flows(flows, vertices)
        print(f"   ✓ Generated {len(ports)} ports from flows")

        # Build graphs from GraphParameters
        graph_name = source_mp.stem
        graphs[graph_name] = {
            'component_id': graph_name,
            'component_type': 'XXGgraph',
            'name': graph_name,
            'parameters': {}
        }

        for idx, row in graph_param_df.iterrows():
            param_name = self._safe_str(row.get('parameter_name', ''))
            param_value = self._safe_str(row.get('parameter_value', ''))
            if param_name:
                graphs[graph_name]['parameters'][param_name] = param_value

        # Build result
        result = {
            'metadata': {
                'source_file': str(source_mp),
                'parser_version': 'vm_fawn_excel_converter_v1',
                'extraction_timestamp': str(source_mp.stat().st_mtime),
                'vm_fawn_excel_source': str(sheets)
            },
            'raw_content': raw_content[:10000],  # First 10KB for context
            'vertices': vertices,
            'flows': flows,
            'ports': ports,
            'graphs': graphs,
            'summary': {
                'total_vertices': len(vertices),
                'total_flows': len(flows),
                'total_ports': len(ports),
                'total_graphs': len(graphs),
                'components_with_transforms': transform_count,
                'datasets_count': len([v for v in vertices.values() if 'dataset' in v.get('component_id', '')])
            }
        }

        print(f"\n✅ Conversion complete:")
        print(f"   Vertices: {len(vertices)} (Transforms: {transform_count}, Datasets: {result['summary']['datasets_count']})")
        print(f"   Flows: {len(flows)}")
        print(f"   Graphs: {len(graphs)}")

        return result

    def _safe_str(self, value) -> str:
        """Safely convert value to string, handling NaN/None"""
        if pd.isna(value) or value is None:
            return ''
        return str(value).strip()

    def _extract_flows_from_raw(self, raw_content: str, vertices: Dict) -> Dict[str, Any]:
        """
        Extract flows from raw .mp file content

        Looks for flow patterns like:
        - connect component_a.out to component_b.in
        - component_a → component_b
        - from component_a to component_b
        """
        flows = {}
        flow_idx = 0

        # Build component name to ID mapping
        component_names = {v['name']: k for k, v in vertices.items()}

        # Pattern 1: "connect X.port to Y.port"
        connect_pattern = r'connect\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s+to\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'
        for match in re.finditer(connect_pattern, raw_content, re.IGNORECASE):
            from_comp, from_port, to_comp, to_port = match.groups()

            # Try to find vertex IDs
            from_vertex = component_names.get(from_comp, from_comp)
            to_vertex = component_names.get(to_comp, to_comp)

            flow_id = f"flow_{flow_idx}"
            flows[flow_id] = {
                'component_id': str(flow_idx),
                'component_type': 'XXGflow',
                'name': f"flow_{from_comp}_to_{to_comp}",
                'type': 'DATA_FLOW',
                'from_vertex': from_vertex,
                'to_vertex': to_vertex,
                'from_port': from_port,
                'to_port': to_port,
                'flow_type': 'DATA_FLOW',
                'raw_content': f"Flow: {from_comp}:{from_port} → {to_comp}:{to_port}"
            }
            flow_idx += 1

        # Pattern 2: "from X to Y" (simpler pattern)
        from_to_pattern = r'from\s+([a-zA-Z0-9_]+)\s+to\s+([a-zA-Z0-9_]+)'
        for match in re.finditer(from_to_pattern, raw_content, re.IGNORECASE):
            from_comp, to_comp = match.groups()

            # Skip if already found in Pattern 1
            existing = any(
                f.get('from_vertex', '').endswith(from_comp) and f.get('to_vertex', '').endswith(to_comp)
                for f in flows.values()
            )
            if existing:
                continue

            from_vertex = component_names.get(from_comp, from_comp)
            to_vertex = component_names.get(to_comp, to_comp)

            flow_id = f"flow_{flow_idx}"
            flows[flow_id] = {
                'component_id': str(flow_idx),
                'component_type': 'XXGflow',
                'name': f"flow_{from_comp}_to_{to_comp}",
                'type': 'DATA_FLOW',
                'from_vertex': from_vertex,
                'to_vertex': to_vertex,
                'from_port': 'out',
                'to_port': 'in',
                'flow_type': 'DATA_FLOW',
                'raw_content': f"Flow: {from_comp} → {to_comp}"
            }
            flow_idx += 1

        return flows

    def _build_ports_from_flows(self, flows: Dict, vertices: Dict) -> Dict[str, Any]:
        """
        Generate ports from flows

        For each component referenced in flows, create input/output ports
        """
        ports = {}
        port_idx = 0

        # Track which components have which port types
        component_ports = {}  # {component_id: {'in': set(), 'out': set()}}

        # Extract ports from flows
        for flow_id, flow in flows.items():
            from_vertex = flow.get('from_vertex', '')
            to_vertex = flow.get('to_vertex', '')
            from_port = flow.get('from_port', 'out')
            to_port = flow.get('to_port', 'in')

            # Initialize port tracking
            if from_vertex not in component_ports:
                component_ports[from_vertex] = {'in': set(), 'out': set()}
            if to_vertex not in component_ports:
                component_ports[to_vertex] = {'in': set(), 'out': set()}

            # Track ports
            component_ports[from_vertex]['out'].add(from_port)
            component_ports[to_vertex]['in'].add(to_port)

        # Create port objects
        for vertex_id, port_types in component_ports.items():
            vertex_data = vertices.get(vertex_id, {})
            vertex_name = vertex_data.get('name', vertex_id)

            # Create output ports
            for port_name in port_types['out']:
                port_id = f"port_{port_idx}"
                ports[port_id] = {
                    'component_id': port_id,
                    'name': port_name,
                    'type': 'OUTPUT',
                    'vertex': vertex_id,
                    'vertex_name': vertex_name,
                    'layout': vertex_data.get('attributes', {}).get('layout', ''),
                    'port_type': 'OUTPUT'
                }
                port_idx += 1

            # Create input ports
            for port_name in port_types['in']:
                port_id = f"port_{port_idx}"
                ports[port_id] = {
                    'component_id': port_id,
                    'name': port_name,
                    'type': 'INPUT',
                    'vertex': vertex_id,
                    'vertex_name': vertex_name,
                    'layout': vertex_data.get('attributes', {}).get('layout', ''),
                    'port_type': 'INPUT'
                }
                port_idx += 1

        return ports


# CLI usage
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python vm_fawn_excel_converter.py <vm_fawn_excel> <source_mp_file> [output_json]")
        print("\nExample:")
        print("  python vm_fawn_excel_converter.py \\")
        print("    'Input Files/VM_FAWN/bbi_preprocessing_output/265_fileTransferToHadoopServer_2025_11_20_14_18_33.xlsx' \\")
        print("    'Input Files/blade/mp/265_fileTransferToHadoopServer.mp' \\")
        print("    'outputs/parsed_abinitio/265_fileTransferToHadoopServer_components.json'")
        sys.exit(1)

    vm_fawn_excel = sys.argv[1]
    source_mp = sys.argv[2]
    output_json = sys.argv[3] if len(sys.argv) > 3 else None

    converter = VMFAWNExcelConverter()
    result = converter.convert_excel_to_json(vm_fawn_excel, source_mp, output_json)

    print(f"\n📊 Summary: {result['summary']}")
