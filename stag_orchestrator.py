"""
STAG Orchestrator

Coordinates all STAG components to generate comprehensive system comparison reports.

Process:
1. Load system mapping (find Databricks pipeline)
2. Extract source logic (Hadoop or Ab Initio)
3. Extract Databricks logic
4. Abstract to business stages (AI)
5. Generate STTM (AI)
6. Create Excel comparison report
7. Return results

Handles errors gracefully and provides detailed progress logging.
"""

from typing import Dict, List, Any, Optional
from loguru import logger
from pathlib import Path

from .system_mapping_service import SystemMappingService
from .hadoop_logic_extractor import HadoopLogicExtractor
from .abinitio_logic_extractor import AbInitioLogicExtractor
from .databricks_logic_extractor import DatabricksLogicExtractor
from .business_stage_abstractor import BusinessStageAbstractor
from .stag_sttm_generator import STAGSTTMGenerator
from .excel_generator import ExcelGenerator


class STAGOrchestrator:
    """Orchestrate complete STAG comparison workflow"""

    def __init__(
        self,
        indexer=None,
        ai_analyzer=None,
        system_mapping_file: str = "Input Files/system_mapping_layer.json"
    ):
        """
        Initialize STAG Orchestrator

        Args:
            indexer: MultiCollectionIndexer for vector search
            ai_analyzer: AIAnalyzer for AI-powered analysis
            system_mapping_file: Path to system mapping layer JSON
        """
        self.indexer = indexer
        self.ai_analyzer = ai_analyzer

        # Initialize all services
        logger.info("🚀 Initializing STAG Orchestrator...")

        self.system_mapping = SystemMappingService(system_mapping_file)
        self.hadoop_extractor = HadoopLogicExtractor(indexer, ai_analyzer)
        self.abinitio_extractor = AbInitioLogicExtractor(indexer, ai_analyzer)
        self.databricks_extractor = DatabricksLogicExtractor(indexer, ai_analyzer)
        self.business_abstractor = BusinessStageAbstractor(ai_analyzer)
        self.sttm_generator = STAGSTTMGenerator(ai_analyzer, indexer)
        self.excel_generator = ExcelGenerator()

        logger.info("✅ STAG Orchestrator initialized")

    def generate_comparison(
        self,
        source_system: str,
        source_workflow: str,
        databricks_pipeline: Optional[str] = None,
        output_folder: str = "outputs/stag_comparisons"
    ) -> Dict[str, Any]:
        """
        Generate complete comparison report

        Args:
            source_system: "hadoop" or "abinitio"
            source_workflow: Source workflow/graph name
            databricks_pipeline: Optional Databricks pipeline name (will look up if not provided)
            output_folder: Output directory for Excel file

        Returns:
            {
                'success': bool,
                'excel_file': str,
                'source_workflow': str,
                'databricks_pipeline': str,
                'business_stages': List[Dict],
                'sttm_count': int,
                'differences_count': int,
                'errors': List[str]
            }
        """
        logger.info("=" * 80)
        logger.info(f"🎯 STAG COMPARISON: {source_system.upper()} → Databricks")
        logger.info(f"Source Workflow: {source_workflow}")
        logger.info("=" * 80)

        errors = []
        result = {
            'success': False,
            'excel_file': '',
            'source_workflow': source_workflow,
            'databricks_pipeline': '',
            'business_stages': [],
            'sttm_count': 0,
            'differences_count': 0,
            'errors': errors
        }

        try:
            # Step 1: Look up Databricks mapping
            logger.info("\n📋 Step 1: Looking up Databricks mapping...")
            if not databricks_pipeline:
                mapping = self.system_mapping.get_databricks_mapping(source_system, source_workflow)
                if not mapping:
                    # FALLBACK: Use AI + Vector Search to find similar Databricks workflow
                    logger.warning(f"   ⚠ No pre-defined mapping found for {source_system}: {source_workflow}")
                    logger.info(f"   🤖 Using AI to find similar Databricks workflow...")

                    search_result = self._intelligent_workflow_search(source_system, source_workflow)

                    # Check if multiple matches returned (dict response)
                    if isinstance(search_result, dict) and search_result.get('status') == 'multiple_matches':
                        # Return result with multiple_matches status for user selection
                        result['status'] = 'multiple_matches'
                        result['matches'] = search_result['matches']
                        result['success'] = False  # Not yet successful - needs user selection
                        logger.info(f"   🔍 Multiple matches found - user selection required")
                        return result

                    databricks_pipeline = search_result

                    if not databricks_pipeline:
                        error_msg = f"No Databricks mapping found for {source_system}: {source_workflow} (tried mapping file and intelligent search)"
                        logger.error(f"   ❌ {error_msg}")
                        errors.append(error_msg)
                        return result

                    logger.info(f"   ✅ AI found similar workflow: {source_workflow} → {databricks_pipeline}")
                else:
                    databricks_pipelines = mapping['databricks_pipelines']
                    logger.info(f"   ✅ Found mapping: {source_workflow} → {databricks_pipelines}")

                    if len(databricks_pipelines) > 1:
                        logger.info(f"   📌 Multiple Databricks pipelines found ({len(databricks_pipelines)}), will process all of them")
            else:
                logger.info(f"   ℹ Using provided Databricks pipeline: {databricks_pipeline}")
                databricks_pipelines = [databricks_pipeline]  # Wrap single pipeline in list

            # Step 2: Extract source logic (once for all pipelines)
            logger.info(f"\n📊 Step 2: Extracting {source_system} logic...")
            if source_system.lower() == 'hadoop':
                source_logic = self.hadoop_extractor.extract_logic(source_workflow)
            elif source_system.lower() == 'abinitio':
                source_logic = self.abinitio_extractor.extract_logic(source_workflow)
            else:
                error_msg = f"Unknown source system: {source_system}"
                logger.error(f"   ❌ {error_msg}")
                errors.append(error_msg)
                return result

            if not source_logic or source_logic.get('total_jobs', 0) == 0 and source_logic.get('total_steps', 0) == 0:
                logger.warning(f"   ⚠ No logic extracted for {source_workflow} (may not be indexed)")

            # Loop through all Databricks pipelines
            all_excel_files = []
            all_results = []

            for pipeline_index, databricks_pipeline in enumerate(databricks_pipelines, 1):
                pipeline_result = {}
                pipeline_result['databricks_pipeline'] = databricks_pipeline

                if len(databricks_pipelines) > 1:
                    logger.info(f"\n{'='*80}")
                    logger.info(f"🔄 Processing Pipeline {pipeline_index}/{len(databricks_pipelines)}: {databricks_pipeline}")
                    logger.info(f"{'='*80}")

                # Step 3: Extract Databricks logic
                logger.info(f"\n📊 Step 3: Extracting Databricks logic...")
                databricks_logic = self.databricks_extractor.extract_logic(databricks_pipeline)

                if not databricks_logic or databricks_logic.get('total_activities', 0) == 0:
                    logger.warning(f"   ⚠ No logic extracted for {databricks_pipeline} (may not be indexed)")

                # Step 4: Abstract to business stages
                logger.info(f"\n🤖 Step 4: Abstracting to business stages (AI)...")
                business_stages = self.business_abstractor.abstract_to_business_stages(
                    source_logic,
                    databricks_logic
                )

                pipeline_result['business_stages'] = business_stages

                # Count differences
                differences = sum(1 for stage in business_stages if stage.get('comparison') == 'Different')
                pipeline_result['differences_count'] = differences

                logger.info(f"   ✅ Generated {len(business_stages)} business stages ({differences} differences)")

                # Step 5: Generate STTM
                logger.info(f"\n🔗 Step 5: Generating STTM (AI)...")
                sttm_mappings = self.sttm_generator.generate_sttm(
                    source_logic,
                    databricks_logic,
                    source_system
                )

                pipeline_result['sttm_count'] = len(sttm_mappings)
                logger.info(f"   ✅ Generated {len(sttm_mappings)} column mappings")

                # Step 6: Generate Excel report
                logger.info(f"\n📄 Step 6: Generating Excel comparison report...")
                excel_file = self.excel_generator.generate_comparison_excel(
                    source_system=source_system,
                    source_workflow=source_workflow,
                    databricks_pipeline=databricks_pipeline,
                    business_stages=business_stages,
                    source_logic=source_logic,
                    databricks_logic=databricks_logic,
                    sttm_mappings=sttm_mappings,
                    output_folder=output_folder
                )

                pipeline_result['excel_file'] = excel_file
                all_excel_files.append(excel_file)
                all_results.append(pipeline_result)

                logger.info(f"   ✅ Excel file generated: {excel_file}")

            # Aggregate results
            result['databricks_pipelines'] = databricks_pipelines
            result['pipeline_results'] = all_results
            result['excel_files'] = all_excel_files
            result['success'] = True

            # Calculate totals
            total_business_stages = sum(r['business_stages'].__len__() for r in all_results)
            total_sttm = sum(r['sttm_count'] for r in all_results)
            total_differences = sum(r['differences_count'] for r in all_results)

            # Final summary
            logger.info("\n" + "=" * 80)
            logger.info("✅ STAG COMPARISON COMPLETE")
            logger.info("=" * 80)
            logger.info(f"Source: {source_system}: {source_workflow}")
            logger.info(f"Databricks Pipelines Processed: {len(databricks_pipelines)}")
            for idx, pipeline_result in enumerate(all_results, 1):
                logger.info(f"  {idx}. {pipeline_result['databricks_pipeline']}")
                logger.info(f"     - Business Stages: {len(pipeline_result['business_stages'])}")
                logger.info(f"     - STTM Mappings: {pipeline_result['sttm_count']}")
                logger.info(f"     - Differences: {pipeline_result['differences_count']}")
                logger.info(f"     - Excel: {pipeline_result['excel_file']}")
            logger.info(f"\nTotals Across All Pipelines:")
            logger.info(f"  - Business Stages: {total_business_stages}")
            logger.info(f"  - STTM Mappings: {total_sttm}")
            logger.info(f"  - Differences: {total_differences}")
            logger.info("=" * 80)

            return result

        except Exception as e:
            error_msg = f"STAG comparison failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            import traceback
            logger.error(traceback.format_exc())
            errors.append(error_msg)
            return result

    def _intelligent_workflow_search(self, source_system: str, source_workflow: str, interactive: bool = True):
        """
        Use AI + vector search to find similar Databricks workflow

        Args:
            source_system: "hadoop" or "abinitio"
            source_workflow: Source workflow name
            interactive: If True, return dict for user selection when multiple matches found

        Returns:
            str: Databricks workflow name
            OR dict: {'status': 'multiple_matches', 'matches': [...]} for user selection
            OR None: No matches found
        """
        if not self.indexer:
            logger.warning("   ⚠ No indexer available for intelligent search")
            return None

        try:
            # Search for similar Databricks workflows using vector search
            query = f"Find Databricks notebook or pipeline similar to {source_system} workflow: {source_workflow}"

            # Use indexer's search_multi_collection method
            search_results = self.indexer.search_multi_collection(
                query=query,
                collections=["databricks_collection"],
                top_k=10  # Increased for better candidate selection
            )

            results = search_results.get('databricks_collection', [])

            # SAFETY CHECK: Ensure results is a list (fix for slice error)
            if not isinstance(results, list):
                logger.error(f"   ❌ Search results is not a list: {type(results)}")
                return None

            if not results:
                logger.warning("   ⚠ No Databricks workflows found in vector database")
                return None

            logger.info(f"   🔍 Found {len(results)} candidate Databricks workflows")

            # Use AI to rank and select best match
            if self.ai_analyzer and self.ai_analyzer.enabled:
                # Build candidate list (safely slice results)
                workflow_candidates = []
                for i, r in enumerate(results[:10]):
                    workflow_name = r['metadata'].get('workflow_name') or r['metadata'].get('file_path', '').split('/')[-1].replace('.json', '')
                    description = r['metadata'].get('description', '')[:200]
                    score = r.get('score', 0)

                    workflow_candidates.append({
                        'name': workflow_name,
                        'description': description,
                        'score': score,
                        'rank': i + 1
                    })

                context = "\n".join([
                    f"{c['rank']}. {c['name']} - {c['description']} (similarity: {c['score']:.3f})"
                    for c in workflow_candidates
                ])

                prompt = f"""Given a {source_system} workflow named "{source_workflow}", analyze these Databricks workflows and rank them by relevance.

Databricks workflow candidates:
{context}

For each workflow, assess if it's a good match (confidence > 0.7). Return a JSON array with confidence scores:

[
  {{"name": "workflow_name", "confidence": 0.95, "reason": "Why it matches"}},
  {{"name": "workflow_name2", "confidence": 0.75, "reason": "Why it matches"}},
  ...
]

Only include workflows with confidence > 0.7. If no good matches, return empty array []."""

                response = self.ai_analyzer.analyze_code(
                    code=prompt,
                    context=f"Workflow matching for {source_system}: {source_workflow}",
                    analysis_type="workflow_extraction"
                )

                # Parse AI ranking
                import json
                import re
                try:
                    json_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', response, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(1)
                    else:
                        json_match = re.search(r'\[.*\]', response, re.DOTALL)
                        if json_match:
                            json_str = json_match.group(0)
                        else:
                            json_str = "[]"

                    ranked_matches = json.loads(json_str)
                    good_matches = [m for m in ranked_matches if m.get('confidence', 0) > 0.7]

                    if len(good_matches) == 0:
                        logger.warning("   ⚠ AI found no good matches")
                        return None
                    elif len(good_matches) == 1:
                        best_match = good_matches[0]['name']
                        logger.info(f"   ✅ AI found single match: {best_match} (confidence: {good_matches[0]['confidence']:.2f})")
                        return best_match
                    else:
                        # Multiple good matches
                        if interactive:
                            logger.info(f"   🔍 Found {len(good_matches)} potential matches - user selection required")
                            return {
                                'status': 'multiple_matches',
                                'source_workflow': source_workflow,
                                'source_system': source_system,
                                'matches': good_matches
                            }
                        else:
                            # Auto-select highest confidence
                            best_match = max(good_matches, key=lambda x: x.get('confidence', 0))
                            logger.info(f"   🤖 AI auto-selected: {best_match['name']} (confidence: {best_match['confidence']:.2f})")
                            return best_match['name']

                except (json.JSONDecodeError, Exception) as e:
                    logger.warning(f"   Failed to parse AI ranking: {e}")

            # Fallback: return highest scored result
            best_result = results[0]
            workflow_name = best_result['metadata'].get('workflow_name') or best_result['metadata'].get('file_path', '').split('/')[-1]
            logger.info(f"   📊 Vector search selected: {workflow_name} (score: {best_result.get('score', 0):.3f})")
            return workflow_name

        except Exception as e:
            logger.error(f"   ❌ Intelligent search failed: {e}")
            return None

    def generate_batch_comparisons(
        self,
        source_system: str,
        output_folder: str = "outputs/stag_comparisons/batch"
    ) -> Dict[str, Any]:
        """
        Generate comparisons for all workflows in a system

        Args:
            source_system: "hadoop" or "abinitio"
            output_folder: Output directory

        Returns:
            {
                'total': int,
                'successful': int,
                'failed': int,
                'results': List[Dict]
            }
        """
        logger.info("=" * 80)
        logger.info(f"📦 BATCH COMPARISON: All {source_system.upper()} workflows")
        logger.info("=" * 80)

        # Get all mappings for system
        mappings = self.system_mapping.get_mappings_by_system(source_system)

        results = {
            'total': len(mappings),
            'successful': 0,
            'failed': 0,
            'results': []
        }

        for i, (workflow, pipelines) in enumerate(mappings.items(), 1):
            logger.info(f"\n[{i}/{len(mappings)}] Processing: {workflow}")

            try:
                result = self.generate_comparison(
                    source_system=source_system,
                    source_workflow=workflow,
                    output_folder=output_folder
                )

                if result['success']:
                    results['successful'] += 1
                    logger.info(f"   ✅ Success: {result['excel_file']}")
                else:
                    results['failed'] += 1
                    logger.error(f"   ❌ Failed: {', '.join(result['errors'])}")

                results['results'].append(result)

            except Exception as e:
                results['failed'] += 1
                logger.error(f"   ❌ Exception: {e}")
                results['results'].append({
                    'success': False,
                    'source_workflow': workflow,
                    'errors': [str(e)]
                })

        # Batch summary
        logger.info("\n" + "=" * 80)
        logger.info("📦 BATCH COMPARISON SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Workflows: {results['total']}")
        logger.info(f"✅ Successful: {results['successful']}")
        logger.info(f"❌ Failed: {results['failed']}")
        logger.info(f"Success Rate: {results['successful']/results['total']*100:.1f}%")
        logger.info("=" * 80)

        return results

    def get_available_comparisons(self) -> Dict[str, List[str]]:
        """
        Get list of available comparisons

        Returns:
            {
                'hadoop': [workflow1, workflow2, ...],
                'abinitio': [workflow1, workflow2, ...]
            }
        """
        hadoop_mappings = self.system_mapping.get_mappings_by_system('hadoop')
        abinitio_mappings = self.system_mapping.get_mappings_by_system('abinitio')

        return {
            'hadoop': list(hadoop_mappings.keys()),
            'abinitio': list(abinitio_mappings.keys())
        }


# Example usage
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("STAG ORCHESTRATOR TEST")
    print("=" * 80)

    # Initialize orchestrator (without indexer/AI for testing)
    orchestrator = STAGOrchestrator()

    print("\n✅ STAG Orchestrator initialized")

    # Get available comparisons
    available = orchestrator.get_available_comparisons()
    print(f"\nAvailable Hadoop workflows: {len(available['hadoop'])}")
    print(f"Available Ab Initio workflows: {len(available['abinitio'])}")

    if available['hadoop']:
        print(f"\nSample Hadoop workflows:")
        for workflow in list(available['hadoop'])[:3]:
            print(f"  - {workflow}")

    if available['abinitio']:
        print(f"\nSample Ab Initio workflows:")
        for workflow in list(available['abinitio'])[:3]:
            print(f"  - {workflow}")

    print("\n" + "=" * 80)
    print("Note: Full comparison requires indexer and AI analyzer")
    print("=" * 80)

    # Test single comparison (will fail without indexer/AI but shows structure)
    print("\nTesting comparison structure (will use fallback methods)...")
    try:
        result = orchestrator.generate_comparison(
            source_system='hadoop',
            source_workflow='cdd: bdf_download',
            output_folder='test_output/stag'
        )

        print(f"\nComparison result:")
        print(f"  Success: {result['success']}")
        print(f"  Excel file: {result['excel_file']}")
        print(f"  Business stages: {len(result['business_stages'])}")
        print(f"  STTM mappings: {result['sttm_count']}")
        print(f"  Differences: {result['differences_count']}")

        if result['errors']:
            print(f"  Errors: {', '.join(result['errors'])}")

    except Exception as e:
        print(f"\nExpected error (no indexer/AI): {e}")

    print("\n" + "=" * 80)
