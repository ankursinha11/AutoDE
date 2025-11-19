"""
AbInitio STTM Generator using VM_Automation

This module wraps the VM_Automation step1, step2, step3 scripts to generate
detailed STTM mappings for AbInitio graphs.

The process:
1. Load parsed JSON from EnhancedAbInitioParser
2. Run step1 to extract graph details
3. Run step2 to embed DML/XFR content
4. Run step3 to generate STTM mapping using STAG's AI analyzer
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from loguru import logger
from datetime import datetime


class AbInitioSTTMGenerator:
    """
    Generate detailed STTM mappings for AbInitio graphs using VM_Automation.

    This integrates the step1, step2, step3 pipeline to analyze
    AbInitio graph components and generate attribute-level mappings.
    Uses STAG's existing AI analyzer instead of a separate Azure OpenAI client.
    """

    def __init__(self, blade_path: str = None, output_folder: str = None, ai_analyzer=None):
        """
        Initialize AbInitio STTM Generator

        Args:
            blade_path: Path to blade directory containing mp, dml, xfr files
            output_folder: Folder to save all outputs
            ai_analyzer: STAG's AI analyzer instance (AIScriptAnalyzer)
        """
        self.blade_path = Path(blade_path) if blade_path else None
        self.output_folder = Path(output_folder) if output_folder else Path("outputs/sttm_abinitio")
        self.ai_analyzer = ai_analyzer

        # Ensure output folder exists
        self.output_folder.mkdir(parents=True, exist_ok=True)

    def generate_sttm_from_parsed_json(
        self,
        parsed_json_path: str,
        output_folder: str = None
    ) -> Dict[str, Any]:
        """
        Generate STTM mapping from parsed JSON components

        This runs the complete VM_Automation pipeline:
        1. step1_extract_graph1_details.py
        2. step2_embed_dml_xfr.py
        3. step3_mapping_optimized.py

        Args:
            parsed_json_path: Path to *_components.json from EnhancedAbInitioParser
            output_folder: Optional output folder override

        Returns:
            Dict with:
            - success: bool
            - excel_file: Path to generated Excel
            - mapping_json: Path to mapping JSON
            - error: Error message if failed
        """
        parsed_path = Path(parsed_json_path)

        if not parsed_path.exists():
            return {
                'success': False,
                'error': f"Parsed JSON not found: {parsed_json_path}"
            }

        output_dir = Path(output_folder) if output_folder else self.output_folder
        output_dir.mkdir(parents=True, exist_ok=True)

        # Extract base filename
        base_filename = parsed_path.stem.replace("_components", "")

        logger.info(f"Generating STTM for: {base_filename}")
        logger.info(f"Output folder: {output_dir}")

        try:
            # Step 1: Extract graph details
            logger.info("Step 1: Extracting graph details...")
            graph_details = self._run_step1(parsed_path, output_dir, base_filename)

            if not graph_details:
                return {
                    'success': False,
                    'error': "Step 1 failed: Could not extract graph details"
                }

            # Step 2: Embed DML/XFR content
            logger.info("Step 2: Embedding DML/XFR content...")
            enriched_graph = self._run_step2(graph_details, output_dir, base_filename)

            if not enriched_graph:
                return {
                    'success': False,
                    'error': "Step 2 failed: Could not embed DML/XFR content"
                }

            # Step 3: Generate STTM mapping with GPT-5
            logger.info("Step 3: Generating STTM mapping with GPT-5...")
            result = self._run_step3(enriched_graph, output_dir, base_filename)

            return result

        except Exception as e:
            logger.error(f"STTM generation failed: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }

    def _run_step1(self, parsed_json_path: Path, output_dir: Path, base_filename: str) -> Optional[Path]:
        """
        Run step1: Extract graph details from parsed components

        Args:
            parsed_json_path: Path to *_components.json
            output_dir: Output directory
            base_filename: Base filename for outputs

        Returns:
            Path to graph details JSON, or None if failed
        """
        try:
            # Import step1 module
            from .step1_extract_graph1_details import GraphDetailExtractor

            # Create extractor
            extractor = GraphDetailExtractor(
                input_json_path=str(parsed_json_path),
                output_folder=output_dir,
                base_filename=base_filename
            )

            # Run extraction
            extractor.run()

            # Return path to output
            output_file = output_dir / f"{base_filename}_detailed_graph1.json"
            if output_file.exists():
                logger.info(f"Step 1 complete: {output_file}")
                return output_file
            else:
                logger.error(f"Step 1 output not found: {output_file}")
                return None

        except ImportError as e:
            logger.warning(f"Step 1 import failed, using simplified extraction: {e}")
            return self._simplified_step1(parsed_json_path, output_dir, base_filename)
        except Exception as e:
            logger.error(f"Step 1 failed: {e}")
            return None

    def _simplified_step1(self, parsed_json_path: Path, output_dir: Path, base_filename: str) -> Optional[Path]:
        """
        Simplified step1 that extracts graph structure from parsed components
        """
        try:
            with open(parsed_json_path, 'r') as f:
                parsed_data = json.load(f)

            # Build graph structure with nested "graph" key (required by step3)
            graph_structure = {
                "graph_id": "1",
                "name": base_filename,
                "level": 0,
                "vertices": [],
                "flows": [],
                "dependencies": []
            }

            # Extract vertices
            for vid, vdata in parsed_data.get('vertices', {}).items():
                graph_structure["vertices"].append({
                    "id": vid,
                    "name": vdata.get('name', 'Unknown'),
                    "type": vdata.get('type', 'Unknown')
                })

            # Extract flows
            for fid, fdata in parsed_data.get('flows', {}).items():
                graph_structure["flows"].append({
                    "id": fid,
                    "source": fdata.get('source', ''),
                    "target": fdata.get('target', '')
                })

            # Wrap in "graph" key for step3 compatibility
            graph_details = {
                "graph": graph_structure,
                "extraction_metadata": {
                    "total_graphs_processed": 1,
                    "main_graph_id": "1",
                    "main_graph_name": base_filename,
                    "extraction_method": "simplified",
                    "timestamp": str(Path(parsed_json_path).stat().st_mtime)
                }
            }

            # Save output
            output_file = output_dir / f"{base_filename}_detailed_graph1.json"
            with open(output_file, 'w') as f:
                json.dump(graph_details, f, indent=2)

            logger.info(f"Simplified step 1 complete: {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Simplified step 1 failed: {e}")
            return None

    def _run_step2(self, graph_details_path: Path, output_dir: Path, base_filename: str) -> Optional[Path]:
        """
        Run step2: Embed DML/XFR content into graph details

        Args:
            graph_details_path: Path to detailed graph JSON from step1
            output_dir: Output directory
            base_filename: Base filename for outputs

        Returns:
            Path to enriched graph JSON, or None if failed
        """
        try:
            # Import step2 module
            from .step2_embed_dml_xfr import DMLXFREmbed

            # Create embedder
            embedder = DMLXFREmbed(
                detailed_graph_path=str(graph_details_path),
                blade_path=str(self.blade_path) if self.blade_path else None,
                output_folder=output_dir,
                base_filename=base_filename
            )

            # Run embedding
            embedder.run()

            # Return path to output
            output_file = output_dir / f"{base_filename}_detailed_graph1_with_files.json"
            if output_file.exists():
                logger.info(f"Step 2 complete: {output_file}")
                return output_file
            else:
                logger.error(f"Step 2 output not found: {output_file}")
                return None

        except ImportError as e:
            logger.warning(f"Step 2 import failed, skipping DML embedding: {e}")
            # Return step1 output as-is
            return graph_details_path
        except Exception as e:
            logger.error(f"Step 2 failed: {e}")
            return None

    def _run_step3(self, enriched_graph_path: Path, output_dir: Path, base_filename: str) -> Dict[str, Any]:
        """
        Run step3: Generate STTM mapping using STAG's AI analyzer

        Args:
            enriched_graph_path: Path to enriched graph JSON from step2
            output_dir: Output directory
            base_filename: Base filename for outputs

        Returns:
            Dict with success status and output paths
        """
        try:
            # Import step3 module
            from .step3_mapping_optimized import MappingGenerator

            # Create generator with STAG's AI analyzer
            generator = MappingGenerator(
                detailed_graph_path=str(enriched_graph_path),
                output_folder=output_dir,
                base_filename=base_filename,
                ai_analyzer=self.ai_analyzer  # Pass STAG's AI analyzer
            )

            # Run mapping generation
            generator.run()

            # Return paths to outputs
            excel_file = output_dir / f"{base_filename}_source_to_target_mapping.xlsx"
            mapping_json = output_dir / f"{base_filename}_final_mapping_optimized.json"

            result = {
                'success': True,
                'base_filename': base_filename,
                'output_folder': str(output_dir)
            }

            if excel_file.exists():
                result['excel_file'] = str(excel_file)
                logger.info(f"Excel file generated: {excel_file}")

            if mapping_json.exists():
                result['mapping_json'] = str(mapping_json)
                logger.info(f"Mapping JSON generated: {mapping_json}")

            return result

        except ImportError as e:
            logger.error(f"Step 3 import failed: {e}")
            return {
                'success': False,
                'error': f"Step 3 import failed: {e}"
            }
        except Exception as e:
            logger.error(f"Step 3 failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def generate_sttm_for_graph(
        self,
        graph_name: str,
        parsed_abinitio_folder: str = "outputs/parsed_abinitio"
    ) -> Dict[str, Any]:
        """
        Convenience method to generate STTM for a graph by name

        Args:
            graph_name: Name of the graph (e.g., "1300_CDD_PatientAcctsXRefPermID")
            parsed_abinitio_folder: Folder containing parsed JSON files

        Returns:
            Result dict from generate_sttm_from_parsed_json
        """
        # Find the parsed JSON file
        parsed_folder = Path(parsed_abinitio_folder)

        # Try exact match first
        parsed_json = parsed_folder / f"{graph_name}_components.json"

        if not parsed_json.exists():
            # Try without .pset extension
            graph_name_clean = graph_name.replace(".pset", "").replace(".mp", "")
            parsed_json = parsed_folder / f"{graph_name_clean}_components.json"

        if not parsed_json.exists():
            # Search for partial match
            for f in parsed_folder.glob("*_components.json"):
                if graph_name_clean in f.stem:
                    parsed_json = f
                    break

        if not parsed_json.exists():
            return {
                'success': False,
                'error': f"Parsed JSON not found for graph: {graph_name}"
            }

        return self.generate_sttm_from_parsed_json(str(parsed_json))


# Example usage
if __name__ == "__main__":
    import sys

    # Test with a parsed JSON file
    if len(sys.argv) > 1:
        parsed_json_path = sys.argv[1]
    else:
        parsed_json_path = "outputs/parsed_abinitio/210_extractRecordFormatsFiles_components.json"

    # Note: When used with STAG, pass ai_analyzer from session state:
    # generator = AbInitioSTTMGenerator(
    #     blade_path="Input Files/blade",
    #     output_folder="outputs/sttm_abinitio",
    #     ai_analyzer=st.session_state.ai_analyzer  # Uses STAG's model
    # )

    generator = AbInitioSTTMGenerator(
        blade_path="Input Files/blade",
        output_folder="outputs/sttm_abinitio"
        # ai_analyzer=None means it will use its own GPTLLM
    )

    result = generator.generate_sttm_from_parsed_json(parsed_json_path)

    if result['success']:
        print(f"\nSTTM generation successful!")
        print(f"Excel: {result.get('excel_file', 'N/A')}")
        print(f"JSON: {result.get('mapping_json', 'N/A')}")
    else:
        print(f"\nSTTM generation failed: {result.get('error', 'Unknown error')}")
