"""
Oozie Parser
Parses Oozie workflow.xml and coordinator.xml files
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional, List
from pathlib import Path
from loguru import logger


class OozieParser:
    """Parser for Oozie workflow and coordinator XML files"""

    def __init__(self):
        # Oozie namespaces
        self.namespaces = {
            "": "uri:oozie:workflow:0.5",
            "coordinator": "uri:oozie:coordinator:0.4",
        }

    def parse_workflow(self, workflow_xml_path: str) -> Dict[str, Any]:
        """Parse Oozie workflow.xml"""
        try:
            tree = ET.parse(workflow_xml_path)
            root = tree.getroot()

            workflow = {
                "name": root.get("name"),
                "actions": [],
                "properties": {},
                "start_node": None,
                "end_node": None,
                "kill_node": None,
            }

            # Find start node
            start = root.find("start", self.namespaces)
            if start is not None:
                workflow["start_node"] = start.get("to")

            # Find end node
            end = root.find("end", self.namespaces)
            if end is not None:
                workflow["end_node"] = end.get("name")

            # Find kill node
            kill = root.find("kill", self.namespaces)
            if kill is not None:
                workflow["kill_node"] = kill.get("name")

            # Parse actions
            for action in root.findall("action", self.namespaces):
                action_data = self._parse_action(action)
                if action_data:
                    workflow["actions"].append(action_data)

            # Extract properties from job.properties if exists
            properties_file = Path(workflow_xml_path).parent / "job.properties"
            if properties_file.exists():
                workflow["properties"] = self._parse_properties(properties_file)

            return workflow

        except Exception as e:
            logger.error(f"Error parsing workflow XML {workflow_xml_path}: {e}")
            return {}

    def parse_coordinator(self, coordinator_xml_path: str) -> Dict[str, Any]:
        """Parse Oozie coordinator.xml"""
        try:
            tree = ET.parse(coordinator_xml_path)
            root = tree.getroot()

            coordinator = {
                "name": root.get("name"),
                "frequency": root.get("frequency"),
                "start": root.get("start"),
                "end": root.get("end"),
                "timezone": root.get("timezone"),
                "workflows": [],
                "properties": {},
            }

            # Find workflow references
            action = root.find("action", self.namespaces)
            if action is not None:
                workflow = action.find("workflow", self.namespaces)
                if workflow is not None:
                    app_path = workflow.find("app-path", self.namespaces)
                    if app_path is not None:
                        coordinator["workflows"].append(app_path.text)

            # Extract properties
            properties_file = Path(coordinator_xml_path).parent / "job.properties"
            if properties_file.exists():
                coordinator["properties"] = self._parse_properties(properties_file)

            return coordinator

        except Exception as e:
            logger.error(f"Error parsing coordinator XML {coordinator_xml_path}: {e}")
            return {}

    def _parse_action(self, action_element: ET.Element) -> Optional[Dict[str, Any]]:
        """Parse individual action from workflow"""
        action_data = {
            "name": action_element.get("name"),
            "type": None,
            "properties": {},
        }

        # Determine action type and extract details
        # Common action types: spark, java, pig, hive, shell, fs, sub-workflow, etc.
        for child in action_element:
            tag = child.tag.split("}")[-1]  # Remove namespace

            if tag in ["spark", "java", "pig", "hive", "shell", "fs", "sub-workflow"]:
                action_data["type"] = tag
                action_data["properties"] = self._extract_action_properties(child)
                break

        return action_data if action_data["type"] else None

    def _extract_action_properties(self, action_element: ET.Element) -> Dict[str, Any]:
        """Extract properties from action element"""
        props = {}

        for child in action_element:
            tag = child.tag.split("}")[-1]
            if child.text:
                props[tag] = child.text.strip()
            elif len(child):
                # Handle nested elements
                props[tag] = {
                    sub.tag.split("}")[-1]: sub.text for sub in child if sub.text
                }

        return props

    def _parse_properties(self, properties_file: Path) -> Dict[str, str]:
        """Parse .properties file"""
        properties = {}

        try:
            with open(properties_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        properties[key.strip()] = value.strip()

        except Exception as e:
            logger.error(f"Error parsing properties file {properties_file}: {e}")

        return properties
