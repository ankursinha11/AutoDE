"""
Azure Data Factory Pipeline Parser
Parses ADF pipeline JSON files to extract orchestration and data flow
"""

import json
from pathlib import Path
from typing import List, Dict, Optional, Set
from loguru import logger

from core.models import Process, Component, ComponentType, DataFlow, ProcessType


class ADFPipelineParser:
    """Parser for Azure Data Factory pipeline JSON files"""

    def __init__(self):
        self.supported_activities = {
            "DatabricksNotebook": "databricks_notebook",
            "DatabricksSparkJar": "databricks_spark_jar",
            "DatabricksSparkPython": "databricks_spark_python",
            "Copy": "copy_data",
            "ExecutePipeline": "sub_pipeline",
            "Lookup": "lookup",
            "GetMetadata": "get_metadata",
            "SqlServerStoredProcedure": "stored_procedure",
            "WebActivity": "web_activity",
            "ForEach": "for_each",
            "IfCondition": "if_condition",
            "Until": "until_loop",
            "Wait": "wait",
        }

    def parse_pipeline(self, pipeline_path: str) -> Dict[str, any]:
        """Parse ADF pipeline JSON file"""
        pipeline_path = Path(pipeline_path)

        if not pipeline_path.exists():
            logger.warning(f"Pipeline file not found: {pipeline_path}")
            return {"process": None, "components": [], "flows": []}

        try:
            with open(pipeline_path, "r", encoding="utf-8") as f:
                pipeline_json = json.load(f)

            logger.debug(f"Parsing ADF pipeline: {pipeline_path.name}")

            # Extract pipeline properties
            properties = pipeline_json.get("properties", {})

            # Create process
            process = self._create_process_from_pipeline(pipeline_path, properties)

            # Extract activities (components)
            activities = properties.get("activities", [])
            components = self._extract_components(activities, process.id)

            # Extract dependencies (data flows)
            flows = self._extract_data_flows(activities, components)

            return {
                "process": process,
                "components": components,
                "flows": flows,
            }

        except Exception as e:
            logger.error(f"Error parsing ADF pipeline {pipeline_path}: {e}")
            return {"process": None, "components": [], "flows": []}

    def _create_process_from_pipeline(self, pipeline_path: Path, properties: Dict) -> Process:
        """Create Process object from pipeline properties"""
        pipeline_name = pipeline_path.stem

        # Extract parameters
        parameters = properties.get("parameters", {})

        # Extract annotations (tags/metadata)
        annotations = properties.get("annotations", [])

        # Determine process type based on activities
        activities = properties.get("activities", [])
        process_type = self._infer_process_type(activities)

        process = Process(
            id=f"adf_{pipeline_name}",
            name=pipeline_name,
            process_type=process_type,
            system="databricks",
            file_path=str(pipeline_path),
            description=properties.get("description", ""),
            metadata={
                "pipeline_type": "azure_data_factory",
                "parameters": list(parameters.keys()),
                "annotations": annotations,
                "activity_count": len(activities),
            },
        )

        return process

    def _extract_components(self, activities: List[Dict], process_id: str) -> List[Component]:
        """Extract components from pipeline activities"""
        components = []

        for activity in activities:
            component = self._parse_activity(activity, process_id)
            if component:
                components.append(component)

        return components

    def _parse_activity(self, activity: Dict, process_id: str) -> Optional[Component]:
        """Parse a single activity into a Component"""
        activity_name = activity.get("name", "unknown")
        activity_type = activity.get("type", "unknown")

        if activity_type not in self.supported_activities:
            logger.debug(f"Unsupported activity type: {activity_type}")
            return None

        # Map activity type to component type
        component_type_map = {
            "DatabricksNotebook": ComponentType.DATABRICKS_NOTEBOOK,
            "DatabricksSparkJar": ComponentType.SPARK_JOB,
            "DatabricksSparkPython": ComponentType.SPARK_JOB,
            "Copy": ComponentType.DATA_COPY,
            "Lookup": ComponentType.LOOKUP,
            "SqlServerStoredProcedure": ComponentType.STORED_PROCEDURE,
        }

        component_type = component_type_map.get(activity_type, ComponentType.CUSTOM)

        # Extract activity-specific details
        type_properties = activity.get("typeProperties", {})

        inputs = []
        outputs = []
        metadata = {"activity_type": activity_type}

        # Parse based on activity type
        if activity_type.startswith("Databricks"):
            # Databricks activity
            notebook_path = type_properties.get("notebookPath", "")
            if notebook_path:
                inputs.append(notebook_path)
                metadata["notebook_path"] = notebook_path

            base_parameters = type_properties.get("baseParameters", {})
            metadata["parameters"] = list(base_parameters.keys())

        elif activity_type == "Copy":
            # Copy activity
            source = type_properties.get("source", {})
            sink = type_properties.get("sink", {})

            # Extract source dataset
            source_dataset = activity.get("inputs", [{}])[0].get("referenceName", "")
            if source_dataset:
                inputs.append(source_dataset)

            # Extract sink dataset
            sink_dataset = activity.get("outputs", [{}])[0].get("referenceName", "")
            if sink_dataset:
                outputs.append(sink_dataset)

            metadata["source_type"] = source.get("type", "")
            metadata["sink_type"] = sink.get("type", "")

        elif activity_type == "Lookup":
            # Lookup activity
            source = type_properties.get("source", {})
            dataset = activity.get("inputs", [{}])[0].get("referenceName", "")
            if dataset:
                inputs.append(dataset)

        # Create component
        component = Component(
            id=f"{process_id}_{activity_name}",
            name=activity_name,
            component_type=component_type,
            system="databricks",
            process_id=process_id,
            input_datasets=inputs,
            output_datasets=outputs,
            metadata=metadata,
        )

        return component

    def _extract_data_flows(self, activities: List[Dict], components: List[Component]) -> List[DataFlow]:
        """Extract data flows based on activity dependencies"""
        flows = []

        # Create component lookup
        component_map = {comp.name: comp for comp in components}

        for activity in activities:
            activity_name = activity.get("name")
            depends_on = activity.get("dependsOn", [])

            for dependency in depends_on:
                dep_activity_name = dependency.get("activity")

                # Find source and target components
                source_comp = component_map.get(dep_activity_name)
                target_comp = component_map.get(activity_name)

                if source_comp and target_comp:
                    flow = DataFlow(
                        source_component_id=source_comp.id,
                        target_component_id=target_comp.id,
                        metadata={
                            "dependency_condition": dependency.get("dependencyConditions", []),
                        },
                    )
                    flows.append(flow)

        return flows

    def _infer_process_type(self, activities: List[Dict]) -> ProcessType:
        """Infer process type from activities"""
        activity_types = [a.get("type") for a in activities]

        # Check for databricks activities
        if any(t.startswith("Databricks") for t in activity_types):
            return ProcessType.DATABRICKS_PIPELINE

        # Check for copy activities
        if "Copy" in activity_types:
            return ProcessType.ETL

        # Check for orchestration activities
        if any(t in ["ForEach", "IfCondition", "Until", "ExecutePipeline"] for t in activity_types):
            return ProcessType.ORCHESTRATION

        return ProcessType.UNKNOWN

    def parse_directory(self, directory_path: str) -> Dict[str, any]:
        """Parse all ADF pipeline files in a directory"""
        directory_path = Path(directory_path)

        if not directory_path.exists():
            logger.warning(f"Directory not found: {directory_path}")
            return {"processes": [], "components": [], "flows": []}

        all_processes = []
        all_components = []
        all_flows = []

        # Find all pipeline JSON files
        pipeline_files = list(directory_path.rglob("pipeline*.json"))

        if not pipeline_files:
            # Try finding any JSON files
            pipeline_files = list(directory_path.rglob("*.json"))

        logger.info(f"Found {len(pipeline_files)} pipeline files in {directory_path}")

        for pipeline_file in pipeline_files:
            result = self.parse_pipeline(str(pipeline_file))

            if result["process"]:
                all_processes.append(result["process"])
                all_components.extend(result["components"])
                all_flows.extend(result["flows"])

        return {
            "processes": all_processes,
            "components": all_components,
            "flows": all_flows,
        }
