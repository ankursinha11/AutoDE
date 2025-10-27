"""
Hadoop Parser
Main parser for Hadoop ecosystems including Oozie, Spark, Pig, Hive, etc.
"""

import os
from typing import List, Dict, Any, Optional
from pathlib import Path
from loguru import logger

from core.models import Component, Process, ComponentType, ProcessType, SystemType, DataFlow
from .oozie_parser import OozieParser
from .spark_parser import SparkParser
from .pig_parser import PigParser
from .hive_parser import HiveParser


class HadoopParser:
    """Main Hadoop codebase parser"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

        # Initialize sub-parsers
        self.oozie_parser = OozieParser()
        self.spark_parser = SparkParser()
        self.pig_parser = PigParser()
        self.hive_parser = HiveParser()

        self.processes: List[Process] = []
        self.components: List[Component] = []
        self.data_flows: List[DataFlow] = []

    def parse_directory(self, directory: str) -> Dict[str, Any]:
        """
        Parse entire Hadoop repository
        Expects structure like:
        - coordinators/ (Oozie coordinators)
        - workflows/ (Oozie workflows with spark/, pig/, hive/, shell/ subdirs)
        - ddl/ (Hive DDL)
        - schema/ (Schema definitions)
        - param/ (Sqoop parameters)
        """
        logger.info(f"Parsing Hadoop directory: {directory}")

        base_path = Path(directory)

        # Parse coordinators (top-level processes)
        coordinator_dir = base_path / "coordinators"
        if coordinator_dir.exists():
            self._parse_coordinators(coordinator_dir)

        # Parse workflows
        workflow_dir = base_path / "workflows"
        if workflow_dir.exists():
            self._parse_workflows(workflow_dir)

        # Parse standalone DDL files
        ddl_dir = base_path / "ddl"
        if ddl_dir.exists():
            self._parse_ddl_dir(ddl_dir)

        logger.info(
            f"Parsed {len(self.processes)} processes, {len(self.components)} components"
        )

        return {
            "processes": self.processes,
            "components": self.components,
            "data_flows": self.data_flows,
            "summary": {
                "total_processes": len(self.processes),
                "total_components": len(self.components),
                "total_flows": len(self.data_flows),
            },
        }

    def _parse_coordinators(self, coordinator_dir: Path):
        """Parse Oozie coordinator files"""
        logger.info(f"Parsing coordinators in {coordinator_dir}")

        # Find all coordinator.xml files
        coordinator_files = list(coordinator_dir.glob("**/*coordinator*.xml"))

        for coord_file in coordinator_files:
            try:
                coordinator = self.oozie_parser.parse_coordinator(str(coord_file))
                if coordinator:
                    # Coordinator is a high-level process
                    process = Process(
                        id=f"hadoop_coordinator_{coord_file.stem}",
                        name=coord_file.stem,
                        system=SystemType.HADOOP,
                        process_type=ProcessType.OOZIE_COORDINATOR,
                        file_path=str(coord_file),
                        description=coordinator.get("description"),
                        schedule=coordinator.get("frequency"),
                        parameters=coordinator.get("properties", {}),
                    )
                    self.processes.append(process)

            except Exception as e:
                logger.error(f"Error parsing coordinator {coord_file}: {e}")

    def _parse_workflows(self, workflow_dir: Path):
        """Parse workflow directories"""
        logger.info(f"Parsing workflows in {workflow_dir}")

        # Each subdirectory is typically a workflow
        workflow_dirs = [d for d in workflow_dir.iterdir() if d.is_dir()]

        for wf_dir in workflow_dirs:
            try:
                self._parse_single_workflow(wf_dir)
            except Exception as e:
                logger.error(f"Error parsing workflow {wf_dir}: {e}")

    def _parse_single_workflow(self, wf_dir: Path):
        """Parse a single workflow directory"""
        logger.info(f"Parsing workflow: {wf_dir.name}")

        # Look for workflow.xml in oozie subdirectory
        oozie_dir = wf_dir / "oozie"
        workflow_xml = None

        if oozie_dir.exists():
            workflow_files = list(oozie_dir.glob("*workflow*.xml"))
            if workflow_files:
                workflow_xml = workflow_files[0]

        # Parse workflow XML
        workflow_data = None
        if workflow_xml:
            workflow_data = self.oozie_parser.parse_workflow(str(workflow_xml))

        # Create process
        process = Process(
            id=f"hadoop_workflow_{wf_dir.name}",
            name=wf_dir.name,
            system=SystemType.HADOOP,
            process_type=ProcessType.OOZIE_WORKFLOW,
            file_path=str(wf_dir),
            description=f"Hadoop workflow: {wf_dir.name}",
            parameters=workflow_data.get("properties", {}) if workflow_data else {},
        )

        # Parse components in subdirectories
        components = []

        # Parse Spark jobs
        spark_dir = wf_dir / "spark"
        if spark_dir.exists():
            components.extend(self._parse_spark_dir(spark_dir, process))

        # Parse Pig scripts
        pig_dir = wf_dir / "pig"
        if pig_dir.exists():
            components.extend(self._parse_pig_dir(pig_dir, process))

        # Parse Hive queries
        hive_dir = wf_dir / "hive"
        if hive_dir.exists():
            components.extend(self._parse_hive_dir(hive_dir, process))

        # Parse shell scripts
        shell_dir = wf_dir / "shell"
        if shell_dir.exists():
            components.extend(self._parse_shell_dir(shell_dir, process))

        # Update process
        process.component_ids = [c.id for c in components]
        process.component_count = len(components)

        # Extract I/O from components
        for comp in components:
            process.input_sources.extend(comp.input_datasets)
            process.output_targets.extend(comp.output_datasets)
            process.tables_involved.extend(comp.tables_read)
            process.tables_involved.extend(comp.tables_written)

        # Deduplicate
        process.input_sources = list(set(process.input_sources))
        process.output_targets = list(set(process.output_targets))
        process.tables_involved = list(set(process.tables_involved))

        self.processes.append(process)
        self.components.extend(components)

        logger.info(f"Workflow {wf_dir.name}: {len(components)} components")

    def _parse_spark_dir(self, spark_dir: Path, process: Process) -> List[Component]:
        """Parse Spark Python files"""
        components = []
        spark_files = list(spark_dir.glob("*.py"))

        for spark_file in spark_files:
            try:
                spark_comp = self.spark_parser.parse_file(str(spark_file), process)
                if spark_comp:
                    components.append(spark_comp)
            except Exception as e:
                logger.error(f"Error parsing Spark file {spark_file}: {e}")

        return components

    def _parse_pig_dir(self, pig_dir: Path, process: Process) -> List[Component]:
        """Parse Pig scripts"""
        components = []
        pig_files = list(pig_dir.glob("*.pig"))

        for pig_file in pig_files:
            try:
                pig_comp = self.pig_parser.parse_file(str(pig_file), process)
                if pig_comp:
                    components.append(pig_comp)
            except Exception as e:
                logger.error(f"Error parsing Pig file {pig_file}: {e}")

        return components

    def _parse_hive_dir(self, hive_dir: Path, process: Process) -> List[Component]:
        """Parse Hive SQL files"""
        components = []
        hive_files = list(hive_dir.glob("*.sql")) + list(hive_dir.glob("*.hql"))

        for hive_file in hive_files:
            try:
                hive_comp = self.hive_parser.parse_file(str(hive_file), process)
                if hive_comp:
                    components.append(hive_comp)
            except Exception as e:
                logger.error(f"Error parsing Hive file {hive_file}: {e}")

        return components

    def _parse_shell_dir(self, shell_dir: Path, process: Process) -> List[Component]:
        """Parse shell scripts"""
        components = []
        shell_files = list(shell_dir.glob("*.sh"))

        for shell_file in shell_files:
            try:
                comp = self._parse_shell_script(shell_file, process)
                if comp:
                    components.append(comp)
            except Exception as e:
                logger.error(f"Error parsing shell script {shell_file}: {e}")

        return components

    def _parse_shell_script(self, shell_file: Path, process: Process) -> Optional[Component]:
        """Parse a shell script"""
        with open(shell_file, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        return Component(
            id=f"{process.id}_shell_{shell_file.stem}",
            name=shell_file.stem,
            component_type=ComponentType.SHELL_SCRIPT,
            system="hadoop",
            file_path=str(shell_file),
            process_id=process.id,
            process_name=process.name,
            source_code=content[:2000],  # Limit size
            business_description=f"Shell script: {shell_file.stem}",
        )

    def _parse_ddl_dir(self, ddl_dir: Path):
        """Parse DDL directory"""
        logger.info(f"Parsing DDL in {ddl_dir}")

        ddl_files = list(ddl_dir.glob("*.sql"))

        for ddl_file in ddl_files:
            try:
                # Parse as standalone Hive component
                comp = self.hive_parser.parse_file(str(ddl_file), None)
                if comp:
                    self.components.append(comp)
            except Exception as e:
                logger.error(f"Error parsing DDL {ddl_file}: {e}")
