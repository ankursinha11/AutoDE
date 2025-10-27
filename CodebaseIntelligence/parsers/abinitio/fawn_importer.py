"""
FAWN Importer
Imports FAWN-generated Excel files and converts to our data models
This allows using FAWN as the Ab Initio parser and our tool for rest
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from loguru import logger

from core.models import Component, Process, ComponentType, ProcessType, SystemType, DataFlow


class FAWNImporter:
    """Import FAWN Excel output and convert to our models"""

    def __init__(self):
        self.processes: List[Process] = []
        self.components: List[Component] = []
        self.data_flows: List[DataFlow] = []

    def import_fawn_excel(self, excel_path: str) -> Dict[str, Any]:
        """
        Import FAWN Excel file
        Expected sheets: DataSet, Component&Fields, GraphParameters, (GraphFlow if available)
        """
        logger.info(f"Importing FAWN Excel: {excel_path}")

        excel_file = Path(excel_path)
        if not excel_file.exists():
            raise FileNotFoundError(f"FAWN Excel file not found: {excel_path}")

        # Read all sheets
        sheets = pd.read_excel(excel_path, sheet_name=None)

        logger.info(f"Found sheets: {list(sheets.keys())}")

        # Extract graph name from filename
        graph_name = excel_file.stem

        # Create process
        process = self._create_process_from_fawn(graph_name, sheets)
        self.processes.append(process)

        # Extract components from DataSet sheet
        if "DataSet" in sheets:
            components = self._extract_components_from_dataset(sheets["DataSet"], process)
            self.components.extend(components)

        # Extract graph parameters
        if "GraphParameters" in sheets:
            self._extract_graph_parameters(sheets["GraphParameters"], process)

        # Extract graph flow if available
        if "GraphFlow" in sheets:
            flows = self._extract_graph_flow(sheets["GraphFlow"], self.components)
            self.data_flows.extend(flows)

        # Update process
        process.component_ids = [c.id for c in self.components if c.process_id == process.id]
        process.component_count = len(process.component_ids)

        logger.info(
            f"Imported: {len(self.components)} components, {len(self.data_flows)} flows"
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

    def _create_process_from_fawn(
        self, graph_name: str, sheets: Dict[str, pd.DataFrame]
    ) -> Process:
        """Create Process from FAWN data"""
        import hashlib

        process_id = hashlib.md5(f"fawn_{graph_name}".encode()).hexdigest()[:16]

        # Extract business function from graph name
        business_function = self._infer_business_function(graph_name)

        return Process(
            id=process_id,
            name=graph_name,
            system=SystemType.ABINITIO,
            process_type=ProcessType.GRAPH,
            file_path=None,
            description=f"Ab Initio graph: {graph_name} (imported from FAWN)",
            business_function=business_function,
            metadata={"source": "FAWN"},
        )

    def _extract_components_from_dataset(
        self, dataset_df: pd.DataFrame, process: Process
    ) -> List[Component]:
        """Extract components from DataSet sheet"""
        components = []
        import hashlib

        for idx, row in dataset_df.iterrows():
            comp_name = row.get("CompDisplay Label", f"component_{idx}")
            comp_type_str = row.get("Component Type", "Unknown")
            dataset_names = row.get("DataSet Names", "")
            dml = row.get("DML", "")
            key_param = row.get("Key Parameter Value", None)

            # Generate ID
            comp_id = hashlib.md5(f"{process.id}_{comp_name}".encode()).hexdigest()[:16]

            # Map component type
            comp_type = self._map_component_type(comp_type_str)

            # Parse DML for schema
            schema = self._parse_dml_basic(dml) if dml else None

            # Determine inputs/outputs based on type
            datasets_list = [d.strip() for d in str(dataset_names).split(",") if d.strip()]

            if comp_type == ComponentType.INPUT_FILE:
                input_datasets = datasets_list
                output_datasets = []
            elif comp_type == ComponentType.OUTPUT_FILE:
                input_datasets = []
                output_datasets = datasets_list
            else:
                input_datasets = datasets_list
                output_datasets = []

            component = Component(
                id=comp_id,
                name=comp_name,
                component_type=comp_type,
                system="abinitio",
                process_id=process.id,
                process_name=process.name,
                input_datasets=input_datasets,
                output_datasets=output_datasets,
                input_schema=schema,
                output_schema=schema,
                dml_definition=dml if isinstance(dml, str) else "",
                key_parameter_value=str(key_param) if pd.notna(key_param) else None,
                business_description=f"{comp_type_str}: {comp_name}",
                metadata={"source": "FAWN", "fawn_row": idx},
            )

            components.append(component)

        return components

    def _extract_graph_parameters(self, params_df: pd.DataFrame, process: Process):
        """Extract graph parameters from GraphParameters sheet"""
        if params_df.empty:
            return

        params = {}
        for idx, row in params_df.iterrows():
            param_name = row.get("Parameter", row.get("parameter", f"param_{idx}"))
            param_value = row.get("Value", row.get("value", ""))

            if pd.notna(param_name):
                params[str(param_name)] = str(param_value)

        process.graph_parameters = params
        process.parameters = params

    def _extract_graph_flow(
        self, flow_df: pd.DataFrame, components: List[Component]
    ) -> List[DataFlow]:
        """Extract data flows from GraphFlow sheet (if FAWN generated it)"""
        flows = []

        # Create component lookup
        comp_by_name = {c.name: c for c in components}

        for idx, row in flow_df.iterrows():
            source_name = row.get("Source Component", row.get("source", ""))
            target_name = row.get("Target Component", row.get("target", ""))
            dataset_name = row.get("Dataset", row.get("dataset", None))

            source_comp = comp_by_name.get(source_name)
            target_comp = comp_by_name.get(target_name)

            if source_comp and target_comp:
                flow = DataFlow(
                    source_component_id=source_comp.id,
                    target_component_id=target_comp.id,
                    dataset_name=str(dataset_name) if pd.notna(dataset_name) else None,
                    flow_type="data",
                )
                flows.append(flow)

        return flows

    def _map_component_type(self, comp_type_str: str) -> ComponentType:
        """Map FAWN component type to our ComponentType"""
        type_map = {
            "Input_File": ComponentType.INPUT_FILE,
            "Output_File": ComponentType.OUTPUT_FILE,
            "Lookup_File": ComponentType.LOOKUP_FILE,
            "Transform": ComponentType.TRANSFORM,
            "Join": ComponentType.JOIN,
            "Filter": ComponentType.FILTER,
            "Aggregate": ComponentType.AGGREGATE,
            "Sort": ComponentType.SORT,
            "Rollup": ComponentType.AGGREGATE,
            "Reformat": ComponentType.TRANSFORM,
        }
        return type_map.get(str(comp_type_str), ComponentType.UNKNOWN)

    def _parse_dml_basic(self, dml: str) -> Dict[str, Any]:
        """Basic DML parsing"""
        if not dml or not isinstance(dml, str):
            return None

        # Extract field names (simplified)
        fields = []
        lines = dml.split("\n")
        for line in lines:
            # Look for field definitions
            if ";" in line and not line.strip().startswith("//"):
                parts = line.strip().split()
                if len(parts) >= 2:
                    field_type = parts[0]
                    field_name = parts[1].replace(";", "")
                    fields.append({"name": field_name, "type": field_type})

        return {"fields": fields} if fields else None

    def _infer_business_function(self, graph_name: str) -> Optional[str]:
        """Infer business function from graph name"""
        name_lower = graph_name.lower()

        if "lead" in name_lower or "ipa" in name_lower:
            return "Lead Generation / Patient Account Processing"
        elif "cdd" in name_lower or "charlotte" in name_lower:
            return "CDD / ICH Data Processing"
        elif "gmrn" in name_lower:
            return "Global MRN / Patient Matching"
        elif "coverage" in name_lower:
            return "Coverage Discovery"

        return None
