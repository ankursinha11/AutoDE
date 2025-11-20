"""
Ab Initio ↔ Databricks Fast Comparison Service
==============================================

Specialized service for generating fast Ab Initio to Databricks comparisons
for large graphs (200-1214+ components) using VM_FAWN Excel data directly.

This bypasses the per-component AI analysis bottleneck by:
1. Reading VM_FAWN Excel (already has all Ab Initio transformation data)
2. Reading Databricks parsed data from vector DB or JSON files
3. Doing batched AI matching by transform TYPE (not component instance)
4. Generating Excel with RED highlighting for unmatched Ab Initio logic

Speed: Minutes instead of hours (20-30 API calls vs 1000+)
"""

from typing import Dict, List, Any, Optional
from pathlib import Path
from loguru import logger
import json

from .fast_stag_comparison import FastSTAGComparison


class AbInitioDatabricksComparison:
    """Fast comparison service for Ab Initio ↔ Databricks"""

    def __init__(self, indexer=None, ai_analyzer=None):
        """
        Initialize comparison service

        Args:
            indexer: MultiCollectionIndexer for Databricks data access
            ai_analyzer: AIAnalyzer for semantic matching
        """
        self.indexer = indexer
        self.ai_analyzer = ai_analyzer
        self.fast_comparator = FastSTAGComparison(ai_analyzer=ai_analyzer)

        logger.info("✅ Ab Initio ↔ Databricks Fast Comparison Service initialized")

    def generate_comparison(
        self,
        abinitio_graph_name: str,
        databricks_pipeline: Optional[str] = None,
        output_folder: str = "outputs/stag_comparisons"
    ) -> Dict[str, Any]:
        """
        Generate fast Ab Initio ↔ Databricks comparison

        Args:
            abinitio_graph_name: Ab Initio graph name (e.g., "1500_CDD_TUSourcedFamilyMemberLink")
            databricks_pipeline: Optional Databricks pipeline name
            output_folder: Output directory

        Returns:
            {
                'success': bool,
                'excel_file': str,
                'abinitio_graph': str,
                'databricks_pipeline': str,
                'abinitio_components': int,
                'databricks_transformations': int,
                'matched': int,
                'unmatched': int,
                'match_rate': float,
                'errors': List[str]
            }
        """
        logger.info("=" * 80)
        logger.info("🚀 FAST AB INITIO ↔ DATABRICKS COMPARISON")
        logger.info(f"Ab Initio Graph: {abinitio_graph_name}")
        logger.info("=" * 80)

        errors = []
        result = {
            'success': False,
            'excel_file': '',
            'abinitio_graph': abinitio_graph_name,
            'databricks_pipeline': '',
            'abinitio_components': 0,
            'databricks_transformations': 0,
            'matched': 0,
            'unmatched': 0,
            'match_rate': 0.0,
            'errors': errors
        }

        try:
            # Step 1: Find VM_FAWN Excel for Ab Initio graph
            logger.info("\n📊 Step 1: Finding VM_FAWN Excel...")
            vm_fawn_excel = self._find_vm_fawn_excel(abinitio_graph_name)

            if not vm_fawn_excel:
                error_msg = f"VM_FAWN Excel not found for graph: {abinitio_graph_name}"
                logger.error(f"   ❌ {error_msg}")
                logger.info(f"   💡 Expected location: Input Files/VM_FAWN/bbi_preprocessing_output/{abinitio_graph_name}_*.xlsx")
                logger.info(f"   💡 Make sure to index the graph first: Database → Re-index Ab Initio → Single Graph")
                errors.append(error_msg)
                return result

            logger.info(f"   ✅ Found VM_FAWN Excel: {vm_fawn_excel.name}")

            # Step 2: Find Databricks data
            logger.info("\n📊 Step 2: Finding Databricks data...")
            if not databricks_pipeline:
                # Try to find matching Databricks pipeline using graph name
                databricks_pipeline = self._find_matching_databricks_pipeline(abinitio_graph_name)

                if not databricks_pipeline:
                    error_msg = f"No Databricks pipeline mapping found for: {abinitio_graph_name}"
                    logger.error(f"   ❌ {error_msg}")
                    logger.info(f"   💡 Either specify databricks_pipeline parameter or ensure pipeline is indexed")
                    errors.append(error_msg)
                    return result

                logger.info(f"   ✅ Found matching pipeline: {databricks_pipeline}")
            else:
                logger.info(f"   ℹ️ Using provided pipeline: {databricks_pipeline}")

            result['databricks_pipeline'] = databricks_pipeline

            # Extract Databricks data to JSON format
            databricks_json = self._extract_databricks_to_json(databricks_pipeline, output_folder)

            if not databricks_json:
                error_msg = f"Failed to extract Databricks data for: {databricks_pipeline}"
                logger.error(f"   ❌ {error_msg}")
                errors.append(error_msg)
                return result

            logger.info(f"   ✅ Databricks data extracted: {databricks_json.name}")

            # Step 3: Generate comparison using fast comparison engine
            logger.info("\n🔄 Step 3: Running fast comparison (batched AI analysis)...")

            output_path = Path(output_folder)
            output_path.mkdir(parents=True, exist_ok=True)
            output_excel = output_path / f"{abinitio_graph_name}_vs_{databricks_pipeline.replace('/', '_')}_comparison.xlsx"

            comparison_result = self.fast_comparator.generate_abinitio_databricks_comparison(
                abinitio_vm_fawn_excel=str(vm_fawn_excel),
                databricks_parsed_json=str(databricks_json),
                output_excel=str(output_excel)
            )

            if not comparison_result['success']:
                error_msg = "Fast comparison failed"
                logger.error(f"   ❌ {error_msg}")
                errors.append(error_msg)
                return result

            # Update result
            result['success'] = True
            result['excel_file'] = comparison_result['output_file']
            result['abinitio_components'] = comparison_result['abinitio_components']
            result['databricks_transformations'] = comparison_result['databricks_transformations']
            result['matched'] = comparison_result['matched']
            result['unmatched'] = comparison_result['unmatched']
            result['match_rate'] = comparison_result['match_rate']

            # Final summary
            logger.info("\n" + "=" * 80)
            logger.info("✅ FAST COMPARISON COMPLETE")
            logger.info("=" * 80)
            logger.info(f"Excel File: {result['excel_file']}")
            logger.info(f"Ab Initio Components: {result['abinitio_components']}")
            logger.info(f"Databricks Transformations: {result['databricks_transformations']}")
            logger.info(f"Matched: {result['matched']} ({result['match_rate']:.1f}%)")
            logger.info(f"Unmatched (RED): {result['unmatched']}")
            logger.info("=" * 80)

            return result

        except Exception as e:
            error_msg = f"Fast comparison failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            import traceback
            logger.error(traceback.format_exc())
            errors.append(error_msg)
            return result

    def _find_vm_fawn_excel(self, graph_name: str) -> Optional[Path]:
        """
        Find VM_FAWN Excel for Ab Initio graph

        Args:
            graph_name: Graph name (e.g., "1500_CDD_TUSourcedFamilyMemberLink")

        Returns:
            Path to Excel file or None
        """
        vm_fawn_folder = Path("Input Files/VM_FAWN/bbi_preprocessing_output")

        if not vm_fawn_folder.exists():
            logger.warning(f"   ⚠️ VM_FAWN folder not found: {vm_fawn_folder}")
            return None

        # Look for pattern: {graph_name}_*.xlsx
        matching_files = list(vm_fawn_folder.glob(f"{graph_name}_*.xlsx"))

        if not matching_files:
            logger.warning(f"   ⚠️ No VM_FAWN Excel found for pattern: {graph_name}_*.xlsx")
            return None

        # Return most recent file
        latest_file = max(matching_files, key=lambda p: p.stat().st_mtime)
        return latest_file

    def _find_matching_databricks_pipeline(self, abinitio_graph_name: str) -> Optional[str]:
        """
        Find matching Databricks pipeline for Ab Initio graph

        Uses:
        1. System mapping layer JSON
        2. Vector search (if indexer available)
        3. Name-based heuristics

        Args:
            abinitio_graph_name: Ab Initio graph name

        Returns:
            Databricks pipeline name or None
        """
        # Try system mapping layer first
        mapping_file = Path("Input Files/system_mapping_layer.json")
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r') as f:
                    mappings = json.load(f)

                # Look in Ab Initio mappings
                abinitio_mappings = mappings.get('abinitio_to_databricks', {})
                if abinitio_graph_name in abinitio_mappings:
                    pipelines = abinitio_mappings[abinitio_graph_name].get('databricks_pipelines', [])
                    if pipelines:
                        return pipelines[0]  # Return first match

            except Exception as e:
                logger.warning(f"   ⚠️ Failed to read system mapping: {e}")

        # Fallback: Use vector search if indexer available
        if self.indexer:
            try:
                logger.info(f"   🔍 Using vector search to find matching Databricks pipeline...")
                query = f"Find Databricks pipeline similar to Ab Initio graph: {abinitio_graph_name}"

                results = self.indexer.search_multi_collection(
                    query=query,
                    collections=["databricks_collection"],
                    top_k=3
                )

                if "databricks_collection" in results and results["databricks_collection"]:
                    first_result = results["databricks_collection"][0]
                    metadata = first_result.get('metadata', {})
                    pipeline_name = metadata.get('workflow_name') or metadata.get('file_path', '').split('/')[-1]

                    logger.info(f"   🤖 Vector search found: {pipeline_name}")
                    return pipeline_name

            except Exception as e:
                logger.warning(f"   ⚠️ Vector search failed: {e}")

        # Last resort: Name-based heuristics (CDD graphs → CDD pipelines)
        if "CDD" in abinitio_graph_name.upper():
            # Extract CDD identifier (e.g., "1500_CDD" from "1500_CDD_TUSourcedFamilyMemberLink")
            parts = abinitio_graph_name.split('_')
            if len(parts) >= 2:
                cdd_prefix = '_'.join(parts[:2])  # e.g., "1500_CDD"
                logger.info(f"   💡 Heuristic match: Looking for Databricks pipeline with '{cdd_prefix}'")
                # Return heuristic name (caller will validate)
                return f"CDD/{cdd_prefix}"

        return None

    def _extract_databricks_to_json(
        self,
        pipeline_name: str,
        output_folder: str
    ) -> Optional[Path]:
        """
        Extract Databricks pipeline data to JSON format

        Args:
            pipeline_name: Databricks pipeline name
            output_folder: Output directory

        Returns:
            Path to JSON file or None
        """
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        # Extract just the pipeline base name (remove directory path and extension)
        pipeline_base = Path(pipeline_name).stem  # e.g., "pl_TUSourcedFamilyMemberLink"
        json_file = output_path / f"{pipeline_base}_databricks.json"

        # Check if indexer has Databricks data
        if not self.indexer:
            logger.warning("   ⚠️ No indexer available - cannot extract Databricks data")
            return None

        try:
            # Search for Databricks pipeline
            results = self.indexer.search_multi_collection(
                query=pipeline_name,
                collections=["databricks_collection"],
                top_k=10
            )

            if "databricks_collection" not in results or not results["databricks_collection"]:
                logger.warning(f"   ⚠️ No Databricks data found for: {pipeline_name}")
                return None

            # Extract notebooks and workflows
            notebooks = []
            workflows = []

            for result in results["databricks_collection"]:
                metadata = result.get('metadata', {})
                content = result.get('content', '')

                # Determine type
                file_path = metadata.get('file_path', '')
                if 'notebook' in file_path.lower():
                    notebooks.append({
                        'name': metadata.get('file_name', ''),
                        'path': file_path,
                        'language': metadata.get('language', 'python'),
                        'content': content
                    })
                else:
                    workflows.append({
                        'name': metadata.get('workflow_name', metadata.get('file_name', '')),
                        'path': file_path,
                        'tasks': metadata.get('tasks', []),
                        'content': content
                    })

            # Write to JSON
            databricks_data = {
                'pipeline_name': pipeline_name,
                'notebooks': notebooks,
                'workflows': workflows
            }

            with open(json_file, 'w') as f:
                json.dump(databricks_data, f, indent=2)

            logger.info(f"   ✅ Extracted {len(notebooks)} notebooks, {len(workflows)} workflows")

            return json_file

        except Exception as e:
            logger.error(f"   ❌ Failed to extract Databricks data: {e}")
            return None

    def get_available_comparisons(self) -> List[Dict[str, str]]:
        """
        Get list of available Ab Initio ↔ Databricks comparisons

        Returns:
            List of dicts with 'abinitio_graph' and 'databricks_pipeline' keys
        """
        available = []

        # Check VM_FAWN folder for available graphs
        vm_fawn_folder = Path("Input Files/VM_FAWN/bbi_preprocessing_output")
        if vm_fawn_folder.exists():
            excel_files = list(vm_fawn_folder.glob("*_auto.xlsx"))

            for excel_file in excel_files:
                # Extract graph name (remove _auto.xlsx suffix)
                graph_name = excel_file.stem.replace('_auto', '')

                # Try to find matching Databricks pipeline
                databricks_pipeline = self._find_matching_databricks_pipeline(graph_name)

                available.append({
                    'abinitio_graph': graph_name,
                    'databricks_pipeline': databricks_pipeline or 'Unknown',
                    'vm_fawn_excel': str(excel_file)
                })

        return available


# Example usage
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("FAST AB INITIO ↔ DATABRICKS COMPARISON TEST")
    print("=" * 80)

    # Initialize service (without indexer/AI for testing)
    service = AbInitioDatabricksComparison()

    print("\n✅ Fast Comparison Service initialized")

    # Get available comparisons
    available = service.get_available_comparisons()
    print(f"\nAvailable comparisons: {len(available)}")

    if available:
        print("\nSample available comparisons:")
        for comp in available[:5]:
            print(f"  - {comp['abinitio_graph']} → {comp['databricks_pipeline']}")

    print("\n" + "=" * 80)
    print("Note: Full comparison requires indexer and AI analyzer")
    print("=" * 80)
