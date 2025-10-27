"""
Main Databricks Parser
Orchestrates parsing of Databricks notebooks and ADF pipelines
"""

from pathlib import Path
from typing import List, Dict, Optional
from loguru import logger

from .notebook_parser import NotebookParser
from .adf_parser import ADFPipelineParser
from core.models import Process, Component, DataFlow


class DatabricksParser:
    """
    Main parser for Databricks environment
    Handles notebooks (.py, .sql, .scala, .ipynb) and ADF pipelines
    """

    def __init__(self):
        self.notebook_parser = NotebookParser()
        self.adf_parser = ADFPipelineParser()

    def parse_directory(self, directory_path: str) -> Dict[str, any]:
        """
        Parse a Databricks repository directory
        Looks for notebooks and ADF pipeline definitions

        Args:
            directory_path: Path to the Databricks repository

        Returns:
            Dictionary with processes, components, and flows
        """
        directory_path = Path(directory_path)

        if not directory_path.exists():
            logger.error(f"Directory not found: {directory_path}")
            return {"processes": [], "components": [], "flows": []}

        logger.info(f"Parsing Databricks directory: {directory_path}")

        all_processes = []
        all_components = []
        all_flows = []

        # Parse notebooks
        logger.info("Looking for Databricks notebooks...")
        notebook_components = self._parse_notebooks(directory_path)
        all_components.extend(notebook_components)

        # Create process for each notebook (if not part of pipeline)
        for comp in notebook_components:
            process = self._create_process_from_component(comp)
            all_processes.append(process)

        logger.info(f"Found {len(notebook_components)} notebook components")

        # Parse ADF pipelines
        logger.info("Looking for ADF pipeline definitions...")
        adf_result = self._parse_adf_pipelines(directory_path)

        all_processes.extend(adf_result["processes"])
        all_components.extend(adf_result["components"])
        all_flows.extend(adf_result["flows"])

        logger.info(f"Found {len(adf_result['processes'])} ADF pipelines")

        # Link notebooks to ADF activities
        self._link_notebooks_to_pipelines(notebook_components, adf_result["components"])

        logger.info(f"Total: {len(all_processes)} processes, {len(all_components)} components")

        return {
            "processes": all_processes,
            "components": all_components,
            "flows": all_flows,
        }

    def _parse_notebooks(self, directory_path: Path) -> List[Component]:
        """Parse all notebooks in the directory"""
        components = []

        # Supported notebook extensions
        extensions = ["*.py", "*.sql", "*.scala", "*.ipynb"]

        for ext in extensions:
            notebook_files = list(directory_path.rglob(ext))

            for notebook_file in notebook_files:
                # Skip test files and __pycache__
                if "test" in notebook_file.name.lower() or "__pycache__" in str(notebook_file):
                    continue

                try:
                    notebook_comps = self.notebook_parser.parse_notebook(str(notebook_file))
                    components.extend(notebook_comps)
                except Exception as e:
                    logger.error(f"Error parsing notebook {notebook_file}: {e}")

        return components

    def _parse_adf_pipelines(self, directory_path: Path) -> Dict[str, any]:
        """Parse ADF pipeline definitions"""
        # Look for ADF directory structure
        adf_dirs = [
            directory_path / "adf",
            directory_path / "pipelines",
            directory_path / "datafactory",
            directory_path / ".adf",
        ]

        for adf_dir in adf_dirs:
            if adf_dir.exists():
                logger.info(f"Found ADF directory: {adf_dir}")
                return self.adf_parser.parse_directory(str(adf_dir))

        # If no dedicated ADF directory, look for pipeline JSONs in root
        pipeline_files = list(directory_path.rglob("pipeline*.json"))

        if pipeline_files:
            logger.info(f"Found {len(pipeline_files)} pipeline files in root")
            processes = []
            components = []
            flows = []

            for pipeline_file in pipeline_files:
                result = self.adf_parser.parse_pipeline(str(pipeline_file))
                if result["process"]:
                    processes.append(result["process"])
                    components.extend(result["components"])
                    flows.extend(result["flows"])

            return {"processes": processes, "components": components, "flows": flows}

        return {"processes": [], "components": [], "flows": []}

    def _create_process_from_component(self, component: Component) -> Process:
        """Create a Process object from a standalone notebook component"""
        from core.models import ProcessType

        process = Process(
            id=f"process_{component.id}",
            name=component.name,
            process_type=ProcessType.DATABRICKS_NOTEBOOK,
            system="databricks",
            file_path=component.file_path,
            description=component.business_logic or f"Databricks notebook: {component.name}",
            component_ids=[component.id],
            input_sources=component.input_datasets,
            output_targets=component.output_datasets,
            metadata={
                "notebook_language": component.metadata.get("language", "unknown"),
                "notebook_type": component.metadata.get("notebook_type", "unknown"),
            },
        )

        # Update component with process ID
        component.process_id = process.id

        return process

    def _link_notebooks_to_pipelines(
        self, notebook_components: List[Component], pipeline_components: List[Component]
    ):
        """Link notebook components to their corresponding ADF pipeline activities"""
        # Create lookup by notebook path
        notebook_map = {}
        for comp in notebook_components:
            # Extract notebook name from path
            notebook_name = Path(comp.file_path).stem
            notebook_map[notebook_name] = comp

        # Link pipeline activities to notebooks
        for pipeline_comp in pipeline_components:
            notebook_path = pipeline_comp.metadata.get("notebook_path", "")
            if notebook_path:
                # Extract notebook name
                notebook_name = Path(notebook_path).stem.replace("/", "_")

                # Find matching notebook
                if notebook_name in notebook_map:
                    notebook_comp = notebook_map[notebook_name]

                    # Link them
                    if "linked_pipeline_activity" not in notebook_comp.metadata:
                        notebook_comp.metadata["linked_pipeline_activity"] = []

                    notebook_comp.metadata["linked_pipeline_activity"].append(pipeline_comp.id)

                    pipeline_comp.metadata["linked_notebook"] = notebook_comp.id

                    logger.debug(
                        f"Linked notebook {notebook_name} to pipeline activity {pipeline_comp.name}"
                    )

    def export_to_excel(self, output_path: str):
        """Export parsed results to Excel (similar to FAWN format)"""
        # TODO: Implement Excel export for Databricks
        logger.warning("Excel export for Databricks not yet implemented")
        pass
