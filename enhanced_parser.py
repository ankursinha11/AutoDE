#!/usr/bin/env python3
"""
Enhanced Ab Initio parser with complete component extraction including control/data flow analysis
"""

import re
import json
import os
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Dict, List, Any, Optional
from collections import defaultdict

@dataclass
class Port:
    component_id: str
    component_type: str
    vertex_id: str
    port_name: str
    port_index: str
    is_input: bool
    raw_content: str

@dataclass
class FlowConnection:
    component_id: str
    component_type: str
    port_id: str
    flow_id: str
    is_source: bool
    raw_content: str

@dataclass
class PortBinding:
    component_id: str
    component_type: str
    port1_id: str
    port2_id: str
    raw_content: str

@dataclass
class GraphInfo:
    component_id: str
    component_type: str
    name: str
    raw_content: str

@dataclass
class Legend:
    component_id: str
    component_type: str
    text: str
    raw_content: str

class EnhancedAbInitioParser:
    def __init__(self):
        self.graphs = {}
        self.vertices = {}
        self.flows = {}
        self.ports = {}
        self.flow_connections = {}
        self.port_bindings = {}
        self.graph_vertex_links = {}
        self.graph_flow_links = {}
        self.graph_info = {}
        self.legends = {}
        self.constants = {}
        
        # Enhanced patterns to capture all component types
        self.patterns = {
            # Core components
            'vertex': r'\{(\d+)\|XXGfvertex\|',
            'pvertex': r'\{(\d+)\|XXGpvertex\|',
            'flow': r'\{(\d+)\|XXGflow\|',
            'graph': r'\{(\d+)\|XXGgraph\|',
            
            # Port components
            'iport': r'\{(\d+)\|XXGiport\|',
            'oport': r'\{(\d+)\|XXGoport\|',
            'vertex_input_port': r'\{(\d+)\|XXGvertex_iport_iport\|',
            'vertex_output_port': r'\{(\d+)\|XXGvertex_oport_oport\|',
            
            # Flow connections
            'iport_src_flow': r'\{(\d+)\|XXGiport_src_flow\|',
            'oport_dst_flow': r'\{(\d+)\|XXGoport_dst_flow\|',
            
            # Port bindings
            'iport_binding': r'\{(\d+)\|XXGiport_binding_iport\|',
            'oport_binding': r'\{(\d+)\|XXGoport_binding_oport\|',
            
            # Graph relationships
            'graph_vertex_vertex': r'\{(\d+)\|XXGgraph_vertex_vertex\|',
            'graph_flow_flow': r'\{(\d+)\|XXGgraph_flow_flow\|',
            
            # Metadata components
            'graphinfo': r'\{(\d+)\|XXGgraphinfo\|',
            'legend': r'\{(\d+)\|XXGlegend\|',
            'face': r'\{(\d+)\|XXGface\|',
            'constant': r'\{(\d+)\|XXGconstant\|',
            'directory': r'\{(\d+)\|XXGdirectory\|',
            'runsettings': r'\{(\d+)\|XXGrunsettings\|',
        }
    
    def parse_mp_file(self, file_path: str, output_folder: str = "output", output_filename: str = None) -> Dict[str, Any]:
        """Enhanced parsing with complete component extraction"""
        with open(file_path, 'r', encoding='latin-1') as fp:
            content = fp.read()

        # Store the raw file content for VM_Automation compatibility
        self.raw_mp_content = content
        self.source_file_path = file_path

        print("🔍 ENHANCED PARSING - EXTRACTING ALL COMPONENTS")
        print("=" * 60)

        # Extract all component types
        self._extract_graphs(content)
        self._extract_vertices(content)
        self._extract_flows(content)
        self._extract_ports_enhanced(content)
        self._extract_flow_connections_enhanced(content)
        self._extract_port_bindings(content)
        self._extract_graph_relationships(content)
        self._extract_metadata_components(content)

        # Analyze flow types
        self._analyze_flow_types()

        # Build result and save to file
        result = self._build_result()
        
        # Generate output filename if not provided
        if output_filename is None:
            input_name = Path(file_path).stem
            output_filename = f"{input_name}_components.json"
        
        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)
        
        # Save to file
        output_path = os.path.join(output_folder, output_filename)
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2)
        
        print(f"\n💾 Enhanced components saved to: {output_path}")
        
        return result
    
    def _extract_graphs(self, content: str):
        """Extract graph components with FIXED name extraction"""
        print("   📊 Extracting graphs with fixed name parsing...")
        
        graph_pattern = r'\{(\d+)\|XXGgraph\|(\d+)\|\d+\|\d+\|\d+\|'
        graph_matches = list(re.finditer(graph_pattern, content))
        
        for i, m in enumerate(graph_matches):
            record_id = m.group(1)
            graph_id = m.group(2)
            
            # Find full block
            start_pos = m.start()
            brace_count = 0
            end_pos = start_pos
            for j, ch in enumerate(content[start_pos:], start_pos):
                if ch == '{':
                    brace_count += 1
                elif ch == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = j + 1
                        break
            
            raw_content = content[start_pos:end_pos]
            
            # FIXED: Extract graph name using correct pattern
            name = self._extract_correct_graph_name(raw_content, graph_id)
            
            # Use unique key with index to avoid overwrites
            unique_key = f"{record_id}_graph_{i}"
            self.graphs[unique_key] = {
                'component_id': graph_id,
                'component_type': 'XXGgraph',
                'name': name,
                'raw_content': raw_content
            }
        
        print(f"      • Found {len(self.graphs)} graphs with corrected names")
    
    def _extract_vertices(self, content: str):
        """Extract both functional and parameter vertices using proper patterns from parser.py"""
        # Functional vertices - use proper pattern to extract vertex ID
        fvertex_pattern = r'\{(\d+)\|XXGfvertex\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|'
        fvertex_matches = list(re.finditer(fvertex_pattern, content))
        
        for i, match in enumerate(fvertex_matches):
            record_id = match.group(1)      # e.g., 2010503005
            vertex_id = match.group(2)      # e.g., 56 - this is the actual vertex ID
            
            # Use unique key to avoid overwriting
            unique_key = f"{record_id}_{i}"
            
            # Find the complete vertex block starting from this match
            start_pos = match.start()
            brace_count = 0
            end_pos = start_pos
            
            for j, char in enumerate(content[start_pos:], start_pos):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = j + 1
                        break
            
            raw_content = content[start_pos:end_pos]
            name = self._extract_vertex_name(raw_content, f"fvertex_{vertex_id}")
            
            self.vertices[unique_key] = {
                'component_id': vertex_id,  # Use the actual vertex ID, not the record ID
                'component_type': 'XXGfvertex',
                'name': name,
                'raw_content': raw_content
            }
        
        # Parameter vertices - use proper pattern to extract vertex ID
        pvertex_pattern = r'\{(\d+)\|XXGpvertex\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|'
        pvertex_matches = list(re.finditer(pvertex_pattern, content))
        
        for i, match in enumerate(pvertex_matches):
            record_id = match.group(1)      # e.g., 2010501005
            vertex_id = match.group(2)      # e.g., 3051 - this is the actual vertex ID
            
            # Use unique key to avoid overwriting
            unique_key = f"{record_id}_{i}"
            
            # Find the complete vertex block starting from this match
            start_pos = match.start()
            brace_count = 0
            end_pos = start_pos
            
            for j, char in enumerate(content[start_pos:], start_pos):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = j + 1
                        break
            
            raw_content = content[start_pos:end_pos]
            name = self._extract_vertex_name(raw_content, f"pvertex_{vertex_id}")
            
            self.vertices[unique_key] = {
                'component_id': vertex_id,  # Use the actual vertex ID, not the record ID
                'component_type': 'XXGpvertex',
                'name': name,
                'raw_content': raw_content
            }
    
    def _extract_flows(self, content: str):
        """Extract flow components using proper patterns from parser.py"""
        # Pattern: {2010210004|XXGflow|21|0|41|0|...}
        flow_pattern = r'\{(\d+)\|XXGflow\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|'
        flow_matches = list(re.finditer(flow_pattern, content))
        
        for i, match in enumerate(flow_matches):
            record_id = match.group(1)      # e.g., 2010210004
            flow_id = match.group(2)        # e.g., 21 - this is the actual flow ID
            
            # Use unique key to avoid overwriting
            unique_key = f"{record_id}_{i}"
            
            # Find the complete flow block
            start_pos = match.start()
            brace_count = 0
            end_pos = start_pos
            
            for j, char in enumerate(content[start_pos:], start_pos):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = j + 1
                        break
            
            raw_content = content[start_pos:end_pos]
            name = self._extract_vertex_name(raw_content, f"flow_{flow_id}")
            
            self.flows[unique_key] = {
                'component_id': flow_id,  # Use the actual flow ID, not the record ID
                'component_type': 'XXGflow',
                'name': name,
                'raw_content': raw_content
            }
    
    def _extract_ports_enhanced(self, content: str):
        """Extract ALL input and output ports using correct .mp format like parser.py"""
        # Extract output ports
        # Pattern: {4846|XXGvertex_oport_oport|0|7869|0|{0|out|}3023|3024|}
        oport_pattern = r'\{(\d+)\|XXGvertex_oport_oport\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|\{(\d+)\|([^}]+)\}(\d+)\|(\d+)\|\}'
        oport_matches = list(re.finditer(oport_pattern, content))
        
        for i, match in enumerate(oport_matches):
            component_id = match.group(1)
            unique_key = f"{component_id}_oport_{i}"
            
            # Extract port details directly from regex groups
            port_index = match.group(6)  # Port index (e.g., 0)
            port_name = match.group(7)   # Port name (e.g., "out")
            vertex_id = match.group(8)   # Vertex ID (e.g., 3023)
            port_id = match.group(9)     # Port ID (e.g., 3024)
            raw_content = match.group(0)
            
            self.ports[unique_key] = {
                'component_id': port_id,  # Use the actual port ID as component_id
                'component_type': 'XXGvertex_oport_oport',
                'raw_content': raw_content,
                'port_name': port_name,
                'vertex_id': vertex_id,
                'port_index': port_index,
                'is_input': False
            }
        
        # Extract input ports
        # Pattern: {4849|XXGvertex_iport_iport|0|7873|0|{0|in|}3023|3025|}
        iport_pattern = r'\{(\d+)\|XXGvertex_iport_iport\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|\{(\d+)\|([^}]+)\}(\d+)\|(\d+)\|\}'
        iport_matches = list(re.finditer(iport_pattern, content))
        
        for i, match in enumerate(iport_matches):
            component_id = match.group(1)
            unique_key = f"{component_id}_iport_{i}"
            
            # Extract port details directly from regex groups
            port_index = match.group(6)  # Port index (e.g., 0)
            port_name = match.group(7)   # Port name (e.g., "in")
            vertex_id = match.group(8)   # Vertex ID (e.g., 3023)
            port_id = match.group(9)     # Port ID (e.g., 3025)
            raw_content = match.group(0)
            
            self.ports[unique_key] = {
                'component_id': port_id,  # Use the actual port ID as component_id
                'component_type': 'XXGvertex_iport_iport',
                'raw_content': raw_content,
                'port_name': port_name,
                'vertex_id': vertex_id,
                'port_index': port_index,
                'is_input': True
            }

    def _extract_flow_connections_enhanced(self, content: str):
        """Extract ALL flow connections between ports and flows using correct .mp format"""
        # Extract output port to flow connections
        # Pattern: {4847|XXGoport_dst_flow|0|7871|0|{0|}3024|22|}
        oport_flow_pattern = r'\{(\d+)\|XXGoport_dst_flow\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|\{([^}]*)\}(\d+)\|(\d+)\|\}'
        oport_flow_matches = list(re.finditer(oport_flow_pattern, content))
        
        for i, match in enumerate(oport_flow_matches):
            component_id = match.group(1)
            unique_key = f"{component_id}_oport_flow_{i}"
            
            # Extract connection details directly from regex groups
            port_id = match.group(7)  # The port ID (e.g., 3024)
            flow_id = match.group(8)  # The flow ID (e.g., 22)
            raw_content = match.group(0)
            
            self.flow_connections[unique_key] = {
                'component_id': component_id,
                'component_type': 'XXGoport_dst_flow',
                'raw_content': raw_content,
                'port_id': port_id,
                'flow_id': flow_id,
                'is_source': False
            }
        
        # Extract input port from flow connections
        # Pattern: {4850|XXGiport_src_flow|0|7875|0|{0|}3025|21|}
        iport_flow_pattern = r'\{(\d+)\|XXGiport_src_flow\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|\{([^}]*)\}(\d+)\|(\d+)\|\}'
        iport_flow_matches = list(re.finditer(iport_flow_pattern, content))
        
        for i, match in enumerate(iport_flow_matches):
            component_id = match.group(1)
            unique_key = f"{component_id}_iport_flow_{i}"
            
            # Extract connection details directly from regex groups
            port_id = match.group(7)  # The port ID (e.g., 3025)
            flow_id = match.group(8)  # The flow ID (e.g., 21)
            raw_content = match.group(0)
            
            self.flow_connections[unique_key] = {
                'component_id': component_id,
                'component_type': 'XXGiport_src_flow',
                'raw_content': raw_content,
                'port_id': port_id,
                'flow_id': flow_id,
                'is_source': True
            }
    
    def _extract_port_bindings(self, content: str):
        """Extract port binding relationships (binds ports across subgraphs) like parser.py"""
        # Initialize binding lists like in parser.py
        self.oport_bindings = []  # list of (a, b) bound output port ids
        self.iport_bindings = []  # list of (a, b) bound input port ids
        
        # Output port bindings - extract the last two numeric fields before closing '|}'
        o_matches = list(re.finditer(r'\{(\d+)\|XXGoport_binding_oport\|[\s\S]*?\}(\d+)\|(\d+)\|\}', content))
        for m in o_matches:
            component_id = m.group(1)
            port_a = m.group(2)
            port_b = m.group(3)
            self.oport_bindings.append((port_a, port_b))
            
            # Also store as objects for compatibility
            key = f"{component_id}_oport_binding_{len(self.port_bindings)}"
            self.port_bindings[key] = {
                'component_id': component_id,
                'component_type': 'XXGoport_binding_oport',
                'source_port_id': port_a,
                'target_port_id': port_b,
                'is_input_binding': False,
                'raw_content': m.group(0)
            }
        
        # Input port bindings - extract the last two numeric fields before closing '|}'
        i_matches = list(re.finditer(r'\{(\d+)\|XXGiport_binding_iport\|[\s\S]*?\}(\d+)\|(\d+)\|\}', content))
        for m in i_matches:
            component_id = m.group(1)
            port_a = m.group(2)
            port_b = m.group(3)
            self.iport_bindings.append((port_a, port_b))
            
            # Also store as objects for compatibility
            key = f"{component_id}_iport_binding_{len(self.port_bindings)}"
            self.port_bindings[key] = {
                'component_id': component_id,
                'component_type': 'XXGiport_binding_iport',
                'source_port_id': port_a,
                'target_port_id': port_b,
                'is_input_binding': True,
                'raw_content': m.group(0)
            }
    
    def _extract_graph_relationships(self, content: str):
        """Extract graph relationship components"""
        
        # Graph-vertex relationships - use proper pattern to capture complete blocks
        gv_pattern = r'\{(\d+)\|XXGgraph_vertex_vertex\|'
        gv_matches = re.finditer(gv_pattern, content)
        
        for i, match in enumerate(gv_matches):
            component_id = match.group(1)
            
            # Find the complete link block
            start_pos = match.start()
            brace_count = 0
            end_pos = start_pos
            
            for j, char in enumerate(content[start_pos:], start_pos):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = j + 1
                        break
            
            raw_content = content[start_pos:end_pos]
            
            # Extract link details like in parser.py
            link_details = re.search(r'\{([^}|]+)\|?\}(\d+)\|(\d+)\|\}', raw_content)
            vertex_name = ""
            graph_id = ""
            vertex_id = ""
            
            if link_details:
                vertex_name = link_details.group(1)
                graph_id = link_details.group(2)
                vertex_id = link_details.group(3)
                
                # Also attach graph_id to vertex object if present (like in parser.py)
                for v_key, vertex in self.vertices.items():
                    if vertex.get('component_id') == vertex_id:
                        vertex['graph_id'] = graph_id
                        break
            
            key = f"{component_id}_gv_{i}"
            self.graph_vertex_links[key] = {
                'component_id': component_id,
                'component_type': 'XXGgraph_vertex_vertex',
                'vertex_name': vertex_name,
                'graph_id': graph_id,
                'vertex_id': vertex_id,
                'raw_content': raw_content
            }
        
        # Graph-flow relationships
        gf_pattern = r'\{(\d+)\|XXGgraph_flow_flow\|'
        gf_matches = re.finditer(gf_pattern, content)
        
        for i, match in enumerate(gf_matches):
            component_id = match.group(1)
            
            # Find the complete link block
            start_pos = match.start()
            brace_count = 0
            end_pos = start_pos
            
            for j, char in enumerate(content[start_pos:], start_pos):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = j + 1
                        break
            
            raw_content = content[start_pos:end_pos]
            
            # Extract link details like in parser.py
            link_details = re.search(r'\{([^}|]+)\|?\}(\d+)\|(\d+)\|\}', raw_content)
            flow_name = ""
            graph_id = ""
            flow_id = ""
            
            if link_details:
                flow_name = link_details.group(1)
                graph_id = link_details.group(2)
                flow_id = link_details.group(3)
            
            key = f"{component_id}_gf_{i}"
            self.graph_flow_links[key] = {
                'component_id': component_id,
                'component_type': 'XXGgraph_flow_flow',
                'flow_name': flow_name,
                'graph_id': graph_id,
                'flow_id': flow_id,
                'raw_content': raw_content
            }
    
    def _extract_metadata_components(self, content: str):
        """Extract metadata components like legends, faces, etc."""
        
        # Graph info
        graphinfo_pattern = r'\{(\d+)\|XXGgraphinfo\|([^}]+)\}'
        graphinfo_matches = re.finditer(graphinfo_pattern, content)
        
        for match in graphinfo_matches:
            component_id = match.group(1)
            raw_content = match.group(0)
            
            key = f"{component_id}_graphinfo_{len(self.graph_info)}"
            self.graph_info[key] = {
                'component_id': component_id,
                'component_type': 'XXGgraphinfo',
                'raw_content': raw_content
            }
        
        # Legends
        legend_pattern = r'\{(\d+)\|XXGlegend\|([^}]+)\}'
        legend_matches = re.finditer(legend_pattern, content)
        
        for match in legend_matches:
            component_id = match.group(1)
            raw_content = match.group(0)
            
            key = f"{component_id}_legend_{len(self.legends)}"
            self.legends[key] = {
                'component_id': component_id,
                'component_type': 'XXGlegend',
                'raw_content': raw_content
            }
    
    def _analyze_flow_types(self):
        """Analyze and classify flow types"""
        
        print(f"\n📊 FLOW TYPE ANALYSIS:")
        print(f"   Total flows: {len(self.flows)}")
        print(f"   Flow connections: {len(self.flow_connections)}")
        

        
        # Initialize config flows collection
        self.config_flows = {}
        
        # Classify flows based on connected ports
        flow_classifications = {}
        
        for flow_key, flow in self.flows.items():
            flow_id = flow['component_id']
            
            # Find connections for this flow
            connected_ports = []
            for conn_key, conn in self.flow_connections.items():
                if conn.get('flow_id') == flow_id:
                    connected_ports.append(conn)
            
            # Classify based on port types and names
            flow_type = self._classify_flow_type(connected_ports, flow_id)
            flow_classifications[flow_key] = flow_type
            

            
            # If it's a config/control flow, add to config_flows
            if flow_type in ['CONTROL_FLOW', 'PARAMETER_FLOW']:
                self.config_flows[flow_key] = {
                    'component_id': flow_id,
                    'flow_type': flow_type,
                    'source_component_id': None,
                    'target_component_id': None,
                    'raw_content': flow.get('raw_content', '')
                }
                
                # Try to determine source and target from connections
                for conn in connected_ports:
                    port_id = conn.get('port_id')
                    if port_id:
                        # Find the vertex for this port
                        for port_key, port_data in self.ports.items():
                            if port_data.get('component_id') == port_id:
                                vertex_id = port_data.get('vertex_id')
                                if vertex_id:
                                    # FIXED: Correct Ab Initio flow semantics based on actual data analysis
                                    # XXGoport_dst_flow: output port connects TO flow (output port SENDS data) -> SOURCE
                                    # XXGiport_src_flow: input port connects FROM flow (input port RECEIVES data) -> TARGET
                                    # For data flow direction: sender → receiver
                                    # CORRECT: Config flows are OPPOSITE to data flows (this is the correct behavior)
                                    # Ab Initio config flows flow in reverse direction to data flows
                                    if conn.get('is_source'):
                                        # XXGiport_src_flow (is_source=True): input port receives data -> but for config, vertex should be SOURCE
                                        self.config_flows[flow_key]['source_component_id'] = vertex_id
                                    else:
                                        # XXGoport_dst_flow (is_source=False): output port sends data -> but for config, vertex should be TARGET  
                                        self.config_flows[flow_key]['target_component_id'] = vertex_id
                                break
        
        # Count flow types
        type_counts = defaultdict(int)
        for flow_type in flow_classifications.values():
            type_counts[flow_type] += 1
        
        print(f"   📋 FLOW TYPE DISTRIBUTION:")
        for flow_type, count in type_counts.items():
            print(f"      • {flow_type}: {count}")
        
        print(f"   📋 CONFIG FLOWS IDENTIFIED: {len(self.config_flows)}")
        
        # Add classification to flows
        for flow_key, flow_type in flow_classifications.items():
            self.flows[flow_key]['flow_type'] = flow_type
    
    def _classify_flow_type(self, connected_ports: List[Dict], flow_id: str) -> str:
        """Classify flow type based on connected ports and vertices"""
        
        if not connected_ports:
            return 'UNCONNECTED'
        
        # Analyze connected vertices and port names
        connected_vertices = []
        port_names = []
        
        for conn in connected_ports:
            port_id = conn.get('port_id')
            # Find port details
            for port_key, port in self.ports.items():
                if port.get('component_id') == port_id:
                    port_names.append(port.get('port_name', '').lower())
                    vertex_id = port.get('vertex_id')
                    if vertex_id:
                        # Find vertex details
                        for vertex_key, vertex in self.vertices.items():
                            if vertex.get('component_id') == vertex_id:
                                connected_vertices.append(vertex)
                                break
                    break
        
        # Check if any connected vertex is a parameter vertex (indicates config flow)
        has_pvertex = any(v.get('component_type') == 'XXGpvertex' for v in connected_vertices)
        
        # Classification logic
        if any('error' in name or 'reject' in name or 'fail' in name for name in port_names):
            return 'ERROR_FLOW'
        elif any('log' in name or 'audit' in name or 'trace' in name for name in port_names):
            return 'LOG_FLOW'
        elif has_pvertex or any('param' in name or 'config' in name or 'control' in name for name in port_names):
            return 'PARAMETER_FLOW' if has_pvertex else 'CONTROL_FLOW'
        elif any('condition' in name for name in port_names):
            return 'CONTROL_FLOW'
        else:
            return 'DATA_FLOW'
    
    def _extract_vertex_name(self, raw_content: str, fallback: str) -> str:
        """Extract vertex name from raw content using improved patterns from parser.py"""
        # Prefer pattern near the tail: |<id>|<Name>||@
        tail_matches = re.findall(r'\|(\d+)\|([A-Za-z_][A-Za-z0-9_]*)\|\|@', raw_content)
        if tail_matches:
            return tail_matches[-1][1]
        
        # Previous heuristic
        name_match = re.search(r'\|([a-zA-Z_][a-zA-Z0-9_]*)\|\|@', raw_content)
        if name_match:
            return name_match.group(1)
        
        # Try @Name@ pattern
        middle_matches = re.findall(r'@([a-zA-Z_][a-zA-Z0-9_]{3,})@', raw_content)
        if middle_matches:
            return middle_matches[0]
        
        return fallback
    
    def _extract_correct_graph_name(self, raw_content, graph_id):
        """Extract the correct graph name from raw content"""
        
        # Method 1: Look for pattern after coordinates section with various formats
        # Pattern 1: }}@0|@x|y|0|0|0|0|0|NAME|| (original working pattern)
        coords_pattern1 = r'\}\}@0\|@(\d+\|\d+|\d+)\|0\|0\|0\|0\|0\|([^|]+)\|\|'
        coords_match1 = re.search(coords_pattern1, raw_content)
        if coords_match1:
            candidate_name = coords_match1.group(2).strip()
            if candidate_name and not re.match(r'^\d+$', candidate_name) and candidate_name != '':
                return candidate_name
        
        # Pattern 2: }}@0|@x|y|0|0|0|0|number|NAME|author| (for graphs like 1, 176)
        coords_pattern2 = r'\}\}@0\|@(\d+)\|(\d+)\|0\|0\|0\|0\|(\d+)\|([^|]+)\|([^|]+)\|'
        coords_match2 = re.search(coords_pattern2, raw_content)
        if coords_match2:
            candidate_name = coords_match2.group(4).strip()
            if candidate_name and not re.match(r'^\d+$', candidate_name) and candidate_name != '':
                return candidate_name
        
        # Pattern 3: {}@0|@x|y|0|0|0|0|number|NAME|author| (for graphs like 191, 352, 489)
        coords_pattern3 = r'\{\}@0\|@(\d+)\|(\d+)\|0\|0\|0\|0\|(\d+)\|([^|]+)\|([^|]+)\|'
        coords_match3 = re.search(coords_pattern3, raw_content)
        if coords_match3:
            candidate_name = coords_match3.group(4).strip()
            if candidate_name and not re.match(r'^\d+$', candidate_name) and candidate_name != '':
                return candidate_name
        
        # Method 2: Look for pattern before "Ab Initio"
        ab_initio_pattern = r'\|([^|]+)\|Ab Initio\|'
        ab_initio_match = re.search(ab_initio_pattern, raw_content)
        if ab_initio_match:
            candidate_name = ab_initio_match.group(1).strip()
            if candidate_name and not re.match(r'^\d+$', candidate_name):
                return candidate_name
        
        # Method 3: Look for pattern before timestamp (Created YYYY-MM-DD)
        timestamp_pattern = r'\|([^|]+)\|[^|]*Created \d{1,2}/\d{1,2}/\d{4}'
        timestamp_match = re.search(timestamp_pattern, raw_content)
        if timestamp_match:
            candidate_name = timestamp_match.group(1).strip()
            if candidate_name and not re.match(r'^\d+$', candidate_name):
                return candidate_name
        
        # Method 4: Look for meaningful names in the tail section after coordinates
        tail_name_pattern = r'\|0\|0\|0\|0\|0\|([^|]+)\|[^|]*\|'
        tail_match = re.search(tail_name_pattern, raw_content)
        if tail_match:
            candidate_name = tail_match.group(1).strip()
            if candidate_name and not re.match(r'^\d+$', candidate_name) and len(candidate_name) > 2:
                return candidate_name
        
        # Method 5: Look for meaningful names in the tail section
        tail_section = raw_content.split('@')[-1] if '@' in raw_content else raw_content
        tail_fields = re.findall(r'\|([^|@}]+)\|', tail_section)
        
        meaningful_names = []
        for field in tail_fields:
            field = field.strip()
            if (field and 
                not re.match(r'^\d+$', field) and  # Not just numbers
                not re.match(r'^\d+\.\d+$', field) and  # Not coordinates
                not re.match(r'^Created \d', field) and  # Not timestamp
                field != 'Ab Initio' and  # Not author
                len(field) > 2):  # Meaningful length
                meaningful_names.append(field)
        
        # Return the first meaningful name
        if meaningful_names:
            return meaningful_names[0]
        
        # Method 6: Look for quoted graph names
        quoted_pattern = r'graph\s+"([^"]+)"'
        quoted_match = re.search(quoted_pattern, raw_content)
        if quoted_match:
            return quoted_match.group(1).strip()
        
        # Fallback
        return f"graph_{graph_id}"
    
    def _extract_port_name(self, raw_content: str) -> str:
        """Extract port name from raw content"""
        # Look for port name patterns
        parts = raw_content.split('|')
        
        # Port name is often near the end of the raw content
        for part in reversed(parts):
            if part and not part.isdigit() and len(part) > 0:
                # Clean up the port name
                clean_name = re.sub(r'[{}@]', '', part)
                if clean_name and not clean_name.isdigit():
                    return clean_name
        
        return 'unknown_port'
    
    def _build_result(self) -> Dict[str, Any]:
        """Build the final result dictionary with raw_content for VM_Automation"""

        result = {
            'metadata': {
                'source_file': getattr(self, 'source_file_path', ''),
                'parser_version': 'enhanced_v2_with_raw_content',
                'extraction_timestamp': str(Path(getattr(self, 'source_file_path', '')).stat().st_mtime) if hasattr(self, 'source_file_path') else ''
            },
            'raw_content': getattr(self, 'raw_mp_content', ''),  # Full .mp file content
            'graphs': self.graphs,
            'vertices': self.vertices,
            'flows': self.flows,
            'ports': self.ports,
            'flow_connections': self.flow_connections,
            'port_bindings': self.port_bindings,
            'binding_connections': self.port_bindings,  # Add alias for DOT generator compatibility
            'config_flows': getattr(self, 'config_flows', {}),  # Add config flows
            'graph_vertex_links': self.graph_vertex_links,
            'graph_flow_links': self.graph_flow_links,
            'graph_info': self.graph_info,
            'legends': self.legends,
            'summary': {
                'total_graphs': len(self.graphs),
                'total_vertices': len(self.vertices),
                'total_flows': len(self.flows),
                'total_ports': len(self.ports),
                'total_flow_connections': len(self.flow_connections),
                'total_port_bindings': len(self.port_bindings),
                'total_config_flows': len(getattr(self, 'config_flows', {})),
                'total_graph_vertex_links': len(self.graph_vertex_links),
                'total_graph_flow_links': len(self.graph_flow_links)
            }
        }
        
        print(f"\n✅ ENHANCED PARSING COMPLETE:")
        print(f"   📊 COMPONENT SUMMARY:")
        for key, value in result['summary'].items():
            print(f"      • {key.replace('_', ' ').title()}: {value}")
        
        return result

def main():
    import sys
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python parser.py <input_file> [output_folder] [output_filename]")
        print("Example: python parser.py data/400_commGenIpa.mp")
        print("Example: python parser.py data/400_commGenIpa.mp output")
        print("Example: python parser.py data/400_commGenIpa.mp output custom_components.json")
        return
    
    input_file = sys.argv[1]
    output_folder = sys.argv[2] if len(sys.argv) > 2 else "output"
    output_filename = sys.argv[3] if len(sys.argv) > 3 else None
    
    parser = EnhancedAbInitioParser()
    result = parser.parse_mp_file(input_file, output_folder, output_filename)

if __name__ == "__main__":
    main()