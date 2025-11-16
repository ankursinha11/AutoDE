"""
Business Stage Abstractor

Converts technical workflow steps into business-level stages using AI analysis.
This creates human-readable comparisons for the Overview sheet.

Key Features:
- Multi-level abstraction (technical → business)
- Intelligent stage grouping
- Semantic comparison (not just string matching)
- Context-aware analysis using RAG
- Confidence scoring for comparisons

Uses advanced prompt engineering for optimal RAG results.
"""

from typing import Dict, List, Any, Optional
from loguru import logger
import json
import re
import traceback


class BusinessStageAbstractor:
    """Abstract technical workflow steps to business-level stages using AI"""

    def __init__(self, ai_analyzer=None):
        """
        Initialize Business Stage Abstractor

        Args:
            ai_analyzer: AIAnalyzer for natural language generation
        """
        self.ai_analyzer = ai_analyzer

    def abstract_to_business_stages(
        self,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Abstract technical logic to business stages and compare

        Args:
            source_logic: Logic from Hadoop or Ab Initio
            databricks_logic: Logic from Databricks

        Returns:
            [
                {
                    'stage_name': str,
                    'databricks_description': str,
                    'source_description': str,
                    'comparison': 'Similar' | 'Different',
                    'notes': str,
                    'confidence': float  # 0-1 confidence score
                }
            ]
        """
        logger.info("🤖 Abstracting technical logic to business stages using AI")

        if not self.ai_analyzer:
            logger.warning("No AI analyzer provided - using fallback abstraction")
            return self._fallback_abstraction(source_logic, databricks_logic)

        try:
            # Step 1: Generate business stages with advanced RAG prompting
            business_stages = self._generate_business_stages_with_rag(
                source_logic,
                databricks_logic
            )

            logger.info(f"✅ Generated {len(business_stages)} business stages")
            return business_stages

        except Exception as e:
            logger.error(f"AI abstraction failed: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return self._fallback_abstraction(source_logic, databricks_logic)

    def _generate_business_stages_with_rag(
        self,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generate business stages using advanced RAG techniques

        Advanced prompt engineering for best results:
        1. Chain-of-thought reasoning
        2. Few-shot examples
        3. Structured output format
        4. Context-rich prompts
        5. Explicit comparison criteria
        """

        # Prepare context-rich summaries
        source_summary = self._create_context_rich_summary(source_logic)
        databricks_summary = self._create_context_rich_summary(databricks_logic)

        # Get system names and workflow names, escape curly braces
        source_system_name = str(source_logic.get('system', 'source system')).replace('{', '{{').replace('}', '}}')
        source_workflow_name = str(source_logic.get('workflow_name') or source_logic.get('graph_name', 'Unknown')).replace('{', '{{').replace('}', '}}')
        databricks_pipeline_name = str(databricks_logic.get('pipeline_name', 'Unknown')).replace('{', '{{').replace('}', '}}')

        # Escape any curly braces in summaries to avoid f-string interpolation issues
        source_summary = source_summary.replace('{', '{{').replace('}', '}}')
        databricks_summary = databricks_summary.replace('{', '{{').replace('}', '}}')

        # Advanced RAG prompt with chain-of-thought reasoning
        prompt = f"""You are an expert data engineer analyzing workflow migrations from {source_system_name} to Databricks.

Your task is to identify business-level stages and compare implementations across systems.

# SOURCE SYSTEM WORKFLOW
System: {source_system_name}
Workflow: {source_workflow_name}

{source_summary}

# DATABRICKS WORKFLOW
System: Databricks
Pipeline: {databricks_pipeline_name}

{databricks_summary}

# TASK: Business Stage Analysis

Analyze both workflows and identify business-level stages. Think step-by-step:

1. **Identify Business Stages**: Group technical steps into business-meaningful stages like:
   - Data Ingestion (loading source data)
   - Data Validation (checking data quality)
   - Data Transformation (joins, filters, aggregations)
   - Business Logic Application (calculations, derivations)
   - Data Quality Checks (validation rules)
   - Output Generation (writing results)

2. **Match Equivalent Stages**: Find corresponding stages across both systems
   - Don't rely on exact names - understand the PURPOSE
   - Consider data flow, not just activity names
   - A single stage in one system may map to multiple stages in another

3. **Compare Implementations**: For each stage, determine if implementations are:
   - **Similar**: Same business logic, possibly different technical approach
   - **Different**: Fundamentally different logic or missing in one system

4. **Provide Evidence**: Cite specific jobs/activities/components that support your analysis

# OUTPUT FORMAT

Return a JSON array with this EXACT structure:

```json
[
  {{
    "stage_name": "Data Ingestion",
    "databricks_description": "Detailed description of how Databricks implements this stage. Cite specific activities/notebooks.",
    "source_description": "Detailed description of how source system implements this stage. Cite specific jobs/components.",
    "comparison": "Similar",
    "notes": "Specific differences or observations. Example: 'Both systems read from same source tables, but Databricks adds validation step.'",
    "confidence": 0.95
  }}
]
```

# IMPORTANT GUIDELINES

- **Be Specific**: Don't use generic descriptions. Reference actual job names, activity names, component names.
- **Think Business-Level**: Focus on WHAT is being done (business purpose), not HOW (technical implementation).
- **Accurate Comparison**: "Similar" means same business outcome, even if technical approach differs.
- **Provide Evidence**: Notes should cite specific evidence from the workflows.
- **Confidence Score**: 1.0 = certain, 0.8 = likely, 0.6 = uncertain, 0.4 = guessing

# EXAMPLE (for reference only - analyze the actual workflows above):

```json
[
  {{
    "stage_name": "Customer Data Ingestion",
    "databricks_description": "Activity 'Load_Customer_Data' reads from ADLS customer_raw table using Databricks notebook nb_customer_load.py",
    "source_description": "Pig job 'customer_extract.pig' loads from HDFS /data/customer/raw using HCatalog",
    "comparison": "Similar",
    "notes": "Both systems load customer data from respective storage layers. Databricks uses ADLS while Hadoop uses HDFS, but data schema and business logic are identical.",
    "confidence": 0.95
  }},
  {{
    "stage_name": "Data Quality Validation",
    "databricks_description": "Activity 'Data_Validation' executes notebook nb_validate.py with null checks and schema validation",
    "source_description": "No explicit validation stage found in source workflow",
    "comparison": "Different",
    "notes": "Databricks added new validation stage not present in source system. This is an enhancement.",
    "confidence": 0.90
  }}
]
```

Now analyze the actual workflows above and return the JSON array:
"""

        # Get AI response
        logger.info("   🤖 Sending RAG prompt to AI analyzer...")
        ai_response = self.ai_analyzer.analyze_code(
            code=prompt,
            context="Business stage abstraction",
            analysis_type="workflow_extraction"
        )

        # Parse AI response
        business_stages = self._parse_ai_response_to_stages(ai_response)

        if not business_stages:
            logger.warning("   ⚠ AI returned no stages - using fallback")
            return self._fallback_abstraction(source_logic, databricks_logic)

        # Validate and enrich stages
        business_stages = self._validate_and_enrich_stages(business_stages)

        return business_stages

    def _create_context_rich_summary(self, logic: Dict[str, Any]) -> str:
        """
        Create context-rich summary for better RAG results

        Include:
        - Job/activity/component names
        - Inputs and outputs
        - Transformation descriptions
        - Data flow
        """
        summary_parts = []

        system = logic.get('system', 'Unknown')

        if system == 'hadoop':
            # Hadoop workflow summary
            jobs = logic.get('jobs', [])
            summary_parts.append(f"Total Jobs: {len(jobs)}\n")

            if jobs:
                summary_parts.append("Jobs:")
                for i, job in enumerate(jobs, 1):
                    job_desc = f"\n  {i}. {job.get('name', 'Unknown')}"

                    if job.get('script'):
                        job_desc += f"\n     Script: {job.get('script')}"

                    if job.get('purpose'):
                        job_desc += f"\n     Purpose: {job.get('purpose')}"

                    if job.get('inputs'):
                        job_desc += f"\n     Inputs: {', '.join(job.get('inputs', [])[:3])}"

                    if job.get('outputs'):
                        job_desc += f"\n     Outputs: {', '.join(job.get('outputs', [])[:3])}"

                    if job.get('transformations'):
                        job_desc += f"\n     Transformations: {'; '.join(job.get('transformations', [])[:2])}"

                    summary_parts.append(job_desc)

            # Oozie flow
            oozie_flow = logic.get('oozie_flow', '')
            if oozie_flow and oozie_flow != 'Workflow sequence not found':
                summary_parts.append(f"\nOrchestration Flow: {oozie_flow}")

        elif system == 'abinitio':
            # Ab Initio graph summary
            steps = logic.get('steps', [])
            summary_parts.append(f"Total Steps: {len(steps)}\n")

            if steps:
                summary_parts.append("Transformation Steps:")
                for i, step in enumerate(steps, 1):
                    step_desc = f"\n  {i}. {step.get('dataset', 'Unknown')}"

                    if step.get('component_type'):
                        step_desc += f"\n     Type: {step.get('component_type')}"

                    if step.get('transformation_rules'):
                        step_desc += f"\n     Transformation: {step.get('transformation_rules')}"

                    if step.get('inputs'):
                        step_desc += f"\n     Inputs: {', '.join(step.get('inputs', [])[:3])}"

                    if step.get('outputs'):
                        step_desc += f"\n     Outputs: {', '.join(step.get('outputs', [])[:3])}"

                    summary_parts.append(step_desc)

            # Graph flow
            graph_flow = logic.get('graph_flow', '')
            if graph_flow and graph_flow != 'Empty graph':
                summary_parts.append(f"\nGraph Flow: {graph_flow}")

            # DML files
            dml_files = logic.get('dml_files', [])
            if dml_files:
                summary_parts.append(f"\nDML Files: {', '.join(dml_files[:5])}")

        elif system == 'databricks':
            # Databricks pipeline summary
            activities = logic.get('activities', [])
            summary_parts.append(f"Total Activities: {len(activities)}\n")

            if activities:
                summary_parts.append("Activities:")
                for i, activity in enumerate(activities, 1):
                    activity_desc = f"\n  {i}. {activity.get('name', 'Unknown')}"

                    if activity.get('type'):
                        activity_desc += f"\n     Type: {activity.get('type')}"

                    if activity.get('notebook'):
                        activity_desc += f"\n     Notebook: {activity.get('notebook')}"

                    if activity.get('purpose'):
                        activity_desc += f"\n     Purpose: {activity.get('purpose')}"

                    if activity.get('inputs'):
                        activity_desc += f"\n     Inputs: {', '.join(activity.get('inputs', [])[:3])}"

                    if activity.get('outputs'):
                        activity_desc += f"\n     Outputs: {', '.join(activity.get('outputs', [])[:3])}"

                    if activity.get('transformations'):
                        activity_desc += f"\n     Transformations: {'; '.join(activity.get('transformations', [])[:2])}"

                    summary_parts.append(activity_desc)

            # Orchestration flow
            orch_flow = logic.get('orchestration_flow', '')
            if orch_flow and orch_flow != 'Empty pipeline':
                summary_parts.append(f"\nOrchestration Flow: {orch_flow}")

            # Conditional branches
            branches = logic.get('conditional_branches', {})
            if branches:
                summary_parts.append(f"\nConditional Branches: {len(branches)} conditions")

        return '\n'.join(summary_parts)

    def _parse_ai_response_to_stages(self, ai_response: str) -> List[Dict[str, Any]]:
        """Parse AI response into business stage structures"""
        try:
            # Extract JSON array from response (handles markdown code blocks)
            json_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find JSON array directly
                json_match = re.search(r'\[.*\]', ai_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    logger.warning("No JSON array found in AI response")
                    return []

            stages = json.loads(json_str)

            if isinstance(stages, list):
                logger.info(f"   ✅ Parsed {len(stages)} stages from AI response")
                return stages
            else:
                logger.warning(f"AI response is not a list: {type(stages)}")
                return []

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")
            logger.debug(f"AI response preview: {ai_response[:500]}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error parsing AI response: {e}")
            return []

    def _validate_and_enrich_stages(self, stages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate and enrich business stages

        Ensures:
        - All required fields present
        - Comparison is 'Similar' or 'Different'
        - Confidence is between 0 and 1
        - Descriptions are meaningful (not empty)
        """
        validated_stages = []

        for i, stage in enumerate(stages, 1):
            # Validate required fields
            if not all(key in stage for key in ['stage_name', 'databricks_description', 'source_description', 'comparison']):
                logger.warning(f"   ⚠ Stage {i} missing required fields - skipping")
                continue

            # Validate comparison value
            comparison = stage.get('comparison', '').strip()
            if comparison not in ['Similar', 'Different']:
                logger.warning(f"   ⚠ Stage {i} has invalid comparison '{comparison}' - defaulting to 'Different'")
                stage['comparison'] = 'Different'

            # Validate confidence
            confidence = stage.get('confidence', 0.8)
            if not isinstance(confidence, (int, float)) or not (0 <= confidence <= 1):
                logger.warning(f"   ⚠ Stage {i} has invalid confidence {confidence} - defaulting to 0.8")
                stage['confidence'] = 0.8

            # Ensure notes field exists
            if 'notes' not in stage:
                stage['notes'] = ''

            # Check for meaningful descriptions (not just "Unknown" or empty)
            if len(stage['databricks_description'].strip()) < 10:
                logger.warning(f"   ⚠ Stage {i} has very short Databricks description")

            if len(stage['source_description'].strip()) < 10:
                logger.warning(f"   ⚠ Stage {i} has very short source description")

            validated_stages.append(stage)

        logger.info(f"   ✅ Validated {len(validated_stages)}/{len(stages)} stages")
        return validated_stages

    def _fallback_abstraction(
        self,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Fallback abstraction without AI

        Uses heuristic-based stage identification:
        - Ingestion (input/load/read activities)
        - Transformation (join/filter/transform activities)
        - Validation (validate/check activities)
        - Output (write/store/output activities)
        """
        logger.info("   Using heuristic-based fallback abstraction")

        stages = []

        # Identify common stage patterns
        stage_patterns = {
            'Data Ingestion': ['load', 'read', 'input', 'ingest', 'import', 'extract'],
            'Data Transformation': ['transform', 'join', 'filter', 'map', 'process', 'aggregate'],
            'Data Validation': ['validate', 'check', 'verify', 'quality', 'test'],
            'Business Logic': ['calculate', 'compute', 'derive', 'logic', 'rule'],
            'Data Output': ['write', 'store', 'output', 'export', 'save', 'sink']
        }

        # Get all source jobs/steps
        source_items = self._get_all_items(source_logic)
        databricks_items = self._get_all_items(databricks_logic)

        # Match items to stages
        for stage_name, keywords in stage_patterns.items():
            source_matched = self._match_items_to_stage(source_items, keywords)
            databricks_matched = self._match_items_to_stage(databricks_items, keywords)

            if source_matched or databricks_matched:
                stages.append({
                    'stage_name': stage_name,
                    'databricks_description': databricks_matched if databricks_matched else 'No corresponding stage found',
                    'source_description': source_matched if source_matched else 'No corresponding stage found',
                    'comparison': 'Similar' if (source_matched and databricks_matched) else 'Different',
                    'notes': 'Automated heuristic-based matching (no AI analysis)',
                    'confidence': 0.6  # Lower confidence for heuristic matching
                })

        if not stages:
            # Create generic stage if no matches found
            stages.append({
                'stage_name': 'Data Processing',
                'databricks_description': f"Databricks pipeline with {len(databricks_items)} activities",
                'source_description': f"Source workflow with {len(source_items)} jobs/steps",
                'comparison': 'Different',
                'notes': 'Generic stage created - detailed analysis requires AI',
                'confidence': 0.5
            })

        logger.info(f"   ✅ Created {len(stages)} fallback stages")
        return stages

    def _get_all_items(self, logic: Dict[str, Any]) -> List[str]:
        """Get all job/activity/step names from logic"""
        items = []

        system = logic.get('system', '')

        if system == 'hadoop':
            jobs = logic.get('jobs', [])
            items = [job.get('name', '') for job in jobs]
        elif system == 'abinitio':
            steps = logic.get('steps', [])
            items = [step.get('dataset', '') for step in steps]
        elif system == 'databricks':
            activities = logic.get('activities', [])
            items = [activity.get('name', '') for activity in activities]

        return [item for item in items if item]  # Filter empty strings

    def _match_items_to_stage(self, items: List[str], keywords: List[str]) -> str:
        """Match items to stage based on keywords"""
        matched_items = []

        for item in items:
            item_lower = item.lower()
            if any(keyword in item_lower for keyword in keywords):
                matched_items.append(item)

        if matched_items:
            if len(matched_items) <= 3:
                return f"Includes: {', '.join(matched_items)}"
            else:
                return f"Includes {len(matched_items)} items: {', '.join(matched_items[:3])}, ..."
        else:
            return ''


# Example usage
if __name__ == "__main__":
    # Test without AI
    abstractor = BusinessStageAbstractor()

    print("\n" + "=" * 80)
    print("BUSINESS STAGE ABSTRACTOR TEST")
    print("=" * 80)

    # Sample logic structures
    source_logic = {
        'system': 'hadoop',
        'workflow_name': 'ES_BDF_DOWNLOAD',
        'jobs': [
            {'name': 'Load_Customer_Data', 'purpose': 'Load customer data from HDFS'},
            {'name': 'Transform_Records', 'purpose': 'Transform and filter records'},
            {'name': 'Write_Output', 'purpose': 'Write to output location'}
        ],
        'oozie_flow': 'Load_Customer_Data → Transform_Records → Write_Output'
    }

    databricks_logic = {
        'system': 'databricks',
        'pipeline_name': 'pl_cdd_bdf_download',
        'activities': [
            {'name': 'Ingest_Customer', 'purpose': 'Load customer from ADLS'},
            {'name': 'Apply_Transformations', 'purpose': 'Transform data'},
            {'name': 'Store_Results', 'purpose': 'Write to Delta table'}
        ],
        'orchestration_flow': 'Ingest_Customer → Apply_Transformations → Store_Results'
    }

    print("\nNote: Without AI analyzer, using fallback heuristic matching")

    stages = abstractor.abstract_to_business_stages(source_logic, databricks_logic)

    print(f"\n✅ Generated {len(stages)} business stages:")
    for i, stage in enumerate(stages, 1):
        print(f"\n{i}. {stage['stage_name']}")
        print(f"   Comparison: {stage['comparison']}")
        print(f"   Confidence: {stage['confidence']}")
        print(f"   Databricks: {stage['databricks_description'][:80]}...")
        print(f"   Source: {stage['source_description'][:80]}...")

    print("\n" + "=" * 80)
    print("Note: Full functionality requires AI analyzer for best RAG results")
    print("=" * 80)
