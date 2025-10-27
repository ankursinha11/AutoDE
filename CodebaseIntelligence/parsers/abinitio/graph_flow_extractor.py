"""
Graph Flow Extractor
Extracts data lineage and flow information from Ab Initio graphs
This generates the GraphFlow sheet that FAWN doesn't currently produce
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from loguru import logger

from core.models import Component, DataFlow


class GraphFlowExtractor:
    """Extract data flow/lineage from Ab Initio graphs"""

    def extract_flows(
        self, mp_data: Dict[str, Any], components: List[Component]
    ) -> List[DataFlow]:
        """
        Extract data flows between components
        """
        flows = []

        # Create component lookup
        comp_by_name = {c.name: c for c in components}

        # Method 1: Use explicit graph_flow from MP data
        explicit_flows = mp_data.get("graph_flow", [])
        for flow_def in explicit_flows:
            source_name = flow_def.get("source")
            target_name = flow_def.get("target")

            source_comp = comp_by_name.get(source_name)
            target_comp = comp_by_name.get(target_name)

            if source_comp and target_comp:
                flow = DataFlow(
                    source_component_id=source_comp.id,
                    target_component_id=target_comp.id,
                    dataset_name=flow_def.get("dataset"),
                    flow_type=flow_def.get("type", "data"),
                )
                flows.append(flow)

        # Method 2: Infer flows from component inputs/outputs
        inferred_flows = self._infer_flows_from_datasets(components)
        flows.extend(inferred_flows)

        # Method 3: Infer flows from component types
        type_based_flows = self._infer_flows_from_types(components)
        flows.extend(type_based_flows)

        # Remove duplicates
        flows = self._deduplicate_flows(flows)

        logger.info(f"Extracted {len(flows)} data flows")
        return flows

    def _infer_flows_from_datasets(self, components: List[Component]) -> List[DataFlow]:
        """Infer flows by matching input/output dataset names"""
        flows = []

        # Build dataset maps
        dataset_producers: Dict[str, Component] = {}  # dataset_name -> component
        dataset_consumers: Dict[str, List[Component]] = {}  # dataset_name -> [components]

        for comp in components:
            # Map output datasets to producers
            for dataset in comp.output_datasets:
                if dataset:
                    dataset_producers[dataset] = comp

            # Map input datasets to consumers
            for dataset in comp.input_datasets:
                if dataset:
                    if dataset not in dataset_consumers:
                        dataset_consumers[dataset] = []
                    dataset_consumers[dataset].append(comp)

        # Create flows from producers to consumers
        for dataset_name, producer in dataset_producers.items():
            consumers = dataset_consumers.get(dataset_name, [])
            for consumer in consumers:
                flow = DataFlow(
                    source_component_id=producer.id,
                    target_component_id=consumer.id,
                    dataset_name=dataset_name,
                    flow_type="data",
                )
                flows.append(flow)

        return flows

    def _infer_flows_from_types(self, components: List[Component]) -> List[DataFlow]:
        """Infer flows based on component types and naming patterns"""
        flows = []

        # Typical Ab Initio patterns:
        # Input_File -> Transform/Filter/Join -> Output_File
        # Lookup_File -> Join (as secondary input)

        from core.models import ComponentType

        # Group components by type
        inputs = [c for c in components if c.component_type == ComponentType.INPUT_FILE]
        outputs = [
            c for c in components if c.component_type == ComponentType.OUTPUT_FILE
        ]
        transforms = [
            c
            for c in components
            if c.component_type
            in [
                ComponentType.TRANSFORM,
                ComponentType.FILTER,
                ComponentType.JOIN,
                ComponentType.AGGREGATE,
            ]
        ]
        lookups = [
            c for c in components if c.component_type == ComponentType.LOOKUP_FILE
        ]

        # Pattern 1: Input -> Transforms
        if inputs and transforms:
            # Connect first input to first transform
            if len(inputs) > 0 and len(transforms) > 0:
                flow = DataFlow(
                    source_component_id=inputs[0].id,
                    target_component_id=transforms[0].id,
                    flow_type="data",
                )
                flows.append(flow)

        # Pattern 2: Transforms -> Transforms (chain)
        for i in range(len(transforms) - 1):
            flow = DataFlow(
                source_component_id=transforms[i].id,
                target_component_id=transforms[i + 1].id,
                flow_type="data",
            )
            flows.append(flow)

        # Pattern 3: Last Transform -> Outputs
        if transforms and outputs:
            last_transform = transforms[-1]
            for output in outputs:
                flow = DataFlow(
                    source_component_id=last_transform.id,
                    target_component_id=output.id,
                    flow_type="data",
                )
                flows.append(flow)

        # Pattern 4: Lookups -> Joins
        joins = [c for c in transforms if c.component_type == ComponentType.JOIN]
        if lookups and joins:
            for lookup in lookups:
                for join in joins:
                    # Check if join references this lookup
                    if self._component_references(join, lookup):
                        flow = DataFlow(
                            source_component_id=lookup.id,
                            target_component_id=join.id,
                            flow_type="lookup",
                        )
                        flows.append(flow)

        return flows

    def _component_references(self, comp: Component, referenced: Component) -> bool:
        """Check if component references another component"""
        # Check in transformation logic, parameters, etc.
        ref_name = referenced.name

        if comp.transformation_logic and ref_name in comp.transformation_logic:
            return True

        if comp.source_code and ref_name in comp.source_code:
            return True

        if comp.parameters:
            param_str = str(comp.parameters)
            if ref_name in param_str:
                return True

        return False

    def _deduplicate_flows(self, flows: List[DataFlow]) -> List[DataFlow]:
        """Remove duplicate flows"""
        seen: Set[Tuple[str, str]] = set()
        unique_flows = []

        for flow in flows:
            key = (flow.source_component_id, flow.target_component_id)
            if key not in seen:
                seen.add(key)
                unique_flows.append(flow)

        return unique_flows

    def build_lineage_graph(self, flows: List[DataFlow]) -> Dict[str, Any]:
        """
        Build a directed graph representation of data lineage
        Returns adjacency list and metadata
        """
        # Build adjacency list
        adjacency: Dict[str, List[str]] = {}
        flow_details: Dict[Tuple[str, str], DataFlow] = {}

        for flow in flows:
            source = flow.source_component_id
            target = flow.target_component_id

            if source not in adjacency:
                adjacency[source] = []
            adjacency[source].append(target)

            flow_details[(source, target)] = flow

        return {
            "adjacency": adjacency,
            "flow_details": flow_details,
            "total_flows": len(flows),
        }

    def find_upstream_components(
        self, component_id: str, flows: List[DataFlow]
    ) -> List[str]:
        """Find all upstream (source) components"""
        upstream = []
        for flow in flows:
            if flow.target_component_id == component_id:
                upstream.append(flow.source_component_id)
        return upstream

    def find_downstream_components(
        self, component_id: str, flows: List[DataFlow]
    ) -> List[str]:
        """Find all downstream (target) components"""
        downstream = []
        for flow in flows:
            if flow.source_component_id == component_id:
                downstream.append(flow.target_component_id)
        return downstream
