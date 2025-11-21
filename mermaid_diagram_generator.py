"""
Ab Initio Graph Flow Mermaid Diagram Generator
Generates visual flow diagrams using Mermaid syntax (no external dependencies)

Output: Mermaid markdown diagrams showing data flow with:
- Components with shape syntax
- Flow connections with arrows
- Component types indicated by shapes

Mermaid advantages:
- No external dependencies (unlike Graphviz)
- Renders in browsers, GitHub, many markdown viewers
- Can be embedded in HTML/Streamlit
"""

from pathlib import Path
from typing import Dict, Any, Optional
import logging
import json

logger = logging.getLogger(__name__)


class AbInitioMermaidDiagramGenerator:
    """
    Generates Mermaid flow diagrams from Ab Initio parsed JSON
    """

    # Component type to Mermaid shape mapping
    COMPONENT_SHAPES = {
        'Input_File': '[({})]',          # Cylindrical shape
        'Output_File': '[({})]',         # Cylindrical shape
        'Lookup_File': '[({} - Lookup)]',
        'Join': '{{{} - Join}}',         # Hexagon shape
        'Reformat': '[{} - Reformat]',   # Rectangle
        'Filter_by_Expression': '[/{} - Filter\\]',  # Trapezoid
        'Sort': '[\\{} - Sort/]',        # Inverted trapezoid
        'Dedup_Sorted': '{{{{{}}}}}',    # Hexagon
        'Aggregate': '{{{{{}}}}}',       # Hexagon
        'Rollup': '{{{{{}}}}}',          # Hexagon
        'Broadcast': '[{} - Broadcast]',
        'Gather': '[{} - Gather]',
        'Scan': '[{} - Scan]',
        'Normalize': '[/{} - Normalize\\]',
        'Partition_by_Key': '[{} - Partition]'
    }

    def __init__(self):
        pass

    def generate_diagram(
        self,
        parsed_json_path: str,
        output_folder: str = "outputs/diagrams",
        format: str = "mmd"
    ) -> Dict[str, Any]:
        """
        Generate Mermaid flow diagram from parsed JSON

        Args:
            parsed_json_path: Path to parsed components JSON
            output_folder: Output folder for diagram
            format: Output format (mmd for mermaid, md for markdown with mermaid block)

        Returns:
            Dict with success status and diagram path
        """
        try:
            parsed_json_path = Path(parsed_json_path)
            output_folder = Path(output_folder)
            output_folder.mkdir(parents=True, exist_ok=True)

            if not parsed_json_path.exists():
                return {
                    'success': False,
                    'error': f'Parsed JSON not found: {parsed_json_path}'
                }

            logger.info(f"Generating Mermaid diagram from: {parsed_json_path.name}")

            # Load parsed JSON
            with open(parsed_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            graph_name = parsed_json_path.stem.replace('_components', '')

            # Generate Mermaid syntax
            mermaid_lines = []
            mermaid_lines.append("graph LR")
            mermaid_lines.append(f"    %% Ab Initio Flow Diagram: {graph_name}")
            mermaid_lines.append("")

            # Add vertices (components)
            vertices = data.get('vertices', {})
            logger.info(f"  Adding {len(vertices)} vertices...")

            # Create sanitized node IDs and labels
            node_mapping = {}
            for vid, vdata in vertices.items():
                component_type = vdata.get('type', 'Unknown')
                component_name = vdata.get('name', vid)

                # Sanitize node ID (alphanumeric only)
                sanitized_id = self._sanitize_node_id(vid)
                node_mapping[vid] = sanitized_id

                # Create node label with shape
                label = self._create_node_label(component_name, component_type)
                mermaid_lines.append(f"    {sanitized_id}{label}")

            mermaid_lines.append("")

            # Add flows (edges)
            flows = data.get('flows', {})
            logger.info(f"  Adding {len(flows)} flows...")

            for fid, fdata in flows.items():
                from_vertex = fdata.get('from_vertex')
                to_vertex = fdata.get('to_vertex')
                from_port = fdata.get('from_port', '')
                to_port = fdata.get('to_port', '')

                if from_vertex and to_vertex and from_vertex in node_mapping and to_vertex in node_mapping:
                    from_id = node_mapping[from_vertex]
                    to_id = node_mapping[to_vertex]

                    # Create edge label (port names)
                    edge_label = ''
                    if from_port or to_port:
                        if from_port and to_port:
                            edge_label = f"|{from_port} → {to_port}|"
                        elif from_port:
                            edge_label = f"|{from_port}|"
                        elif to_port:
                            edge_label = f"|{to_port}|"

                        # Truncate long port names
                        if len(edge_label) > 25:
                            edge_label = edge_label[:22] + '...|'

                    # Add edge with arrow
                    if edge_label:
                        mermaid_lines.append(f"    {from_id} -->{edge_label} {to_id}")
                    else:
                        mermaid_lines.append(f"    {from_id} --> {to_id}")

            # Join all lines
            mermaid_content = "\n".join(mermaid_lines)

            # Save to file
            if format == "md":
                # Wrap in markdown code block
                markdown_content = f"# Ab Initio Flow Diagram: {graph_name}\n\n```mermaid\n{mermaid_content}\n```\n"
                output_file = output_folder / f"{graph_name}_flow_diagram.md"
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(markdown_content)
            else:
                # Save as .mmd file
                output_file = output_folder / f"{graph_name}_flow_diagram.mmd"
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(mermaid_content)

            logger.info(f"✅ Mermaid diagram generated: {output_file}")

            return {
                'success': True,
                'diagram_path': str(output_file),
                'format': format,
                'vertices_count': len(vertices),
                'flows_count': len(flows),
                'mermaid_syntax': mermaid_content
            }

        except Exception as e:
            logger.error(f"Mermaid diagram generation failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }

    def _sanitize_node_id(self, node_id: str) -> str:
        """
        Sanitize node ID for Mermaid (alphanumeric and underscore only)
        """
        # Replace non-alphanumeric characters with underscores
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', node_id)
        # Ensure it starts with a letter
        if not sanitized[0].isalpha():
            sanitized = 'N' + sanitized
        return sanitized

    def _create_node_label(self, name: str, type: str) -> str:
        """
        Create Mermaid node label with appropriate shape

        Returns:
            Mermaid node syntax like [Label] or {Label} etc.
        """
        max_len = 30

        if len(name) > max_len:
            name = name[:max_len-3] + '...'

        # Get shape template for this component type
        shape_template = self.COMPONENT_SHAPES.get(type, '[{}]')

        # Create label text
        label_text = f"{name}"

        # Apply shape template
        return shape_template.replace('{}', label_text)

    def generate_html_with_mermaid(
        self,
        parsed_json_path: str,
        output_folder: str = "outputs/diagrams"
    ) -> Optional[str]:
        """
        Generate standalone HTML file with embedded Mermaid diagram

        Args:
            parsed_json_path: Path to parsed components JSON
            output_folder: Output folder for HTML file

        Returns:
            Path to generated HTML file, or None if failed
        """
        try:
            # Generate Mermaid diagram
            result = self.generate_diagram(parsed_json_path, output_folder, format="mmd")

            if not result['success']:
                return None

            mermaid_syntax = result['mermaid_syntax']
            graph_name = Path(parsed_json_path).stem.replace('_components', '')

            # Create HTML with Mermaid.js
            html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Ab Initio Flow Diagram - {graph_name}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
        }}
        .mermaid {{
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stats {{
            margin-top: 20px;
            padding: 10px;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
    </style>
</head>
<body>
    <h1>Ab Initio Flow Diagram: {graph_name}</h1>
    <div class="stats">
        <strong>Statistics:</strong><br>
        Components: {result['vertices_count']}<br>
        Flows: {result['flows_count']}
    </div>
    <div class="mermaid">
{mermaid_syntax}
    </div>
    <script>
        mermaid.initialize({{ startOnLoad: true, theme: 'default' }});
    </script>
</body>
</html>"""

            # Save HTML file
            output_file = Path(output_folder) / f"{graph_name}_flow_diagram.html"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            logger.info(f"✅ HTML diagram generated: {output_file}")
            return str(output_file)

        except Exception as e:
            logger.error(f"HTML diagram generation failed: {e}")
            return None
