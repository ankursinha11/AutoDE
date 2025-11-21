"""
Ab Initio Codebase Scanner
Scans entire project folder to find related artifacts:
- Sub-graphs (.mp)
- Parameter sets (.pset)
- DML files (.dml)
- EME metadata (.eme)
- Layout files
"""

from pathlib import Path
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class AbInitioCodebaseScanner:
    """
    Scans Ab Initio project folder for related artifacts
    """

    def __init__(self, project_root: str):
        """
        Initialize scanner

        Args:
            project_root: Root directory of Ab Initio project (e.g., "Input Files/blade")
        """
        # Convert to absolute path and resolve to handle Windows paths properly
        self.project_root = Path(project_root).resolve()
        logger.info(f"Initialized scanner with project root: {self.project_root}")
        self.artifacts = {
            'graphs': [],
            'psets': [],
            'dml_files': [],
            'eme_files': [],
            'layout_files': [],
            'include_files': []
        }

    def scan(self, graph_path: str = None) -> Dict[str, Any]:
        """
        Scan for all artifacts related to a graph

        Args:
            graph_path: Path to main .mp graph file (optional)

        Returns:
            Dict with discovered artifacts
        """
        if graph_path:
            graph_path = Path(graph_path)
            graph_name = graph_path.stem
            logger.info(f"Scanning codebase for artifacts related to: {graph_name}")
        else:
            graph_name = "all"
            logger.info(f"Scanning entire codebase: {self.project_root}")

        # Verify project root exists
        if not self.project_root.exists():
            logger.warning(f"Project root not found: {self.project_root}")
            logger.warning(f"  Current working directory: {Path.cwd()}")
            logger.warning(f"  Please ensure the Ab Initio codebase folder exists at this location")
            return {
                'project_root': str(self.project_root),
                'artifacts': self.artifacts,
                'statistics': self._get_statistics()
            }

        # Scan for artifacts
        self._scan_graphs()
        self._scan_psets()
        self._scan_dml_files()
        self._scan_eme_files()
        self._scan_layout_files()
        self._scan_include_files()

        logger.info(f"Scan complete: {self._get_statistics()}")

        return {
            'project_root': str(self.project_root),
            'artifacts': self.artifacts,
            'statistics': self._get_statistics()
        }

    def _scan_graphs(self):
        """Find all .mp graph files"""
        try:
            for mp_file in self.project_root.rglob('*.mp'):
                # Skip temp files
                if 'temp' in str(mp_file).lower() or mp_file.name.startswith('.'):
                    continue

                self.artifacts['graphs'].append({
                    'path': str(mp_file),
                    'name': mp_file.stem,
                    'size': mp_file.stat().st_size,
                    'relative_path': str(mp_file.relative_to(self.project_root))
                })
        except Exception as e:
            logger.warning(f"Error scanning graphs: {e}")

    def _scan_psets(self):
        """Find all .pset parameter files"""
        try:
            for pset_file in self.project_root.rglob('*.pset'):
                if pset_file.name.startswith('.'):
                    continue

                self.artifacts['psets'].append({
                    'path': str(pset_file),
                    'name': pset_file.stem,
                    'size': pset_file.stat().st_size,
                    'relative_path': str(pset_file.relative_to(self.project_root))
                })
        except Exception as e:
            logger.warning(f"Error scanning psets: {e}")

    def _scan_dml_files(self):
        """Find all .dml data layout files"""
        try:
            for dml_file in self.project_root.rglob('*.dml'):
                if dml_file.name.startswith('.'):
                    continue

                self.artifacts['dml_files'].append({
                    'path': str(dml_file),
                    'name': dml_file.stem,
                    'size': dml_file.stat().st_size,
                    'relative_path': str(dml_file.relative_to(self.project_root))
                })
        except Exception as e:
            logger.warning(f"Error scanning DML files: {e}")

    def _scan_eme_files(self):
        """Find EME metadata files"""
        try:
            for eme_file in self.project_root.rglob('*.eme'):
                if eme_file.name.startswith('.'):
                    continue

                self.artifacts['eme_files'].append({
                    'path': str(eme_file),
                    'name': eme_file.stem,
                    'size': eme_file.stat().st_size,
                    'relative_path': str(eme_file.relative_to(self.project_root))
                })
        except Exception as e:
            logger.warning(f"Error scanning EME files: {e}")

    def _scan_layout_files(self):
        """Find layout/record format files"""
        try:
            for dat_file in self.project_root.rglob('*.dat'):
                if dat_file.name.startswith('.'):
                    continue

                # Check if it's a layout file (by path or name)
                if 'layout' in str(dat_file).lower() or 'format' in str(dat_file).lower():
                    self.artifacts['layout_files'].append({
                        'path': str(dat_file),
                        'name': dat_file.stem,
                        'size': dat_file.stat().st_size,
                        'relative_path': str(dat_file.relative_to(self.project_root))
                    })
        except Exception as e:
            logger.warning(f"Error scanning layout files: {e}")

    def _scan_include_files(self):
        """Find include files (various extensions)"""
        try:
            for ext in ['.inc', '.h', '.config']:
                for inc_file in self.project_root.rglob(f'*{ext}'):
                    if inc_file.name.startswith('.'):
                        continue

                    self.artifacts['include_files'].append({
                        'path': str(inc_file),
                        'name': inc_file.stem,
                        'size': inc_file.stat().st_size,
                        'relative_path': str(inc_file.relative_to(self.project_root))
                    })
        except Exception as e:
            logger.warning(f"Error scanning include files: {e}")

    def _get_statistics(self) -> Dict[str, int]:
        """Get count statistics"""
        return {
            'graphs': len(self.artifacts['graphs']),
            'psets': len(self.artifacts['psets']),
            'dml_files': len(self.artifacts['dml_files']),
            'eme_files': len(self.artifacts['eme_files']),
            'layout_files': len(self.artifacts['layout_files']),
            'include_files': len(self.artifacts['include_files'])
        }

    def get_artifact_paths(self, artifact_type: str) -> List[str]:
        """
        Get list of paths for a specific artifact type

        Args:
            artifact_type: One of: graphs, psets, dml_files, eme_files, layout_files, include_files

        Returns:
            List of file paths
        """
        if artifact_type not in self.artifacts:
            return []

        return [artifact['path'] for artifact in self.artifacts[artifact_type]]
