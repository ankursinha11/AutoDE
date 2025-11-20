"""
Query Classifier for Intent Detection
======================================

Analyzes user queries to determine intent and route to appropriate agents.

Intent Types:
- SIMPLE_RAG: Direct question answerable from vector search
- COMPARISON: Comparing multiple systems/entities
- LINEAGE: Column-level or data flow questions
- LOGIC_ANALYSIS: Understanding transformation logic
- CODE_SEARCH: Finding specific code patterns
- MULTI_STEP: Complex queries requiring multiple agents
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import re
from loguru import logger


class QueryIntent(Enum):
    """Query intent types"""
    SIMPLE_RAG = "simple_rag"
    COMPARISON = "comparison"
    LINEAGE = "lineage"
    LOGIC_ANALYSIS = "logic_analysis"
    CODE_SEARCH = "code_search"
    MULTI_STEP = "multi_step"
    DOCUMENT_GENERATION = "document_generation"  # NEW: Generate docs, STTM, comparison sheets
    WORKFLOW_MAPPING = "workflow_mapping"  # NEW: Workflow replacement/migration queries
    STAG_COMPARISON = "stag_comparison"  # NEW: Generate full STAG comparison Excel reports
    USER_SELECTION = "user_selection"  # NEW: User selecting from multiple options
    UNKNOWN = "unknown"


@dataclass
class ClassifiedQuery:
    """Result of query classification"""
    intent: QueryIntent  # Primary intent
    confidence: float
    entities: List[str]  # Entities mentioned (graphs, tables, fields)
    systems: List[str]  # Systems mentioned (abinitio, hadoop, databricks)
    keywords: List[str]  # Important keywords
    reasoning: str  # Why this intent was chosen
    suggested_agents: List[str]  # Which agents to use
    task_decomposition: List[str]  # Suggested task breakdown
    secondary_intents: List[QueryIntent]  # Additional intents detected (multi-intent support)
    intent_scores: Dict[QueryIntent, float]  # Confidence scores for all intents


class QueryClassifier:
    """
    Classifies user queries to determine intent and routing strategy
    """

    def __init__(self, ai_analyzer=None):
        """
        Initialize query classifier

        Args:
            ai_analyzer: Optional AI analyzer for semantic understanding
        """
        self.ai_analyzer = ai_analyzer

        # Intent detection patterns
        self.patterns = {
            QueryIntent.COMPARISON: [
                r'\b(compare|difference|versus|vs|similar|equivalent|match)\b',
                r'\b(both|between|across)\b.*\b(system|graph|workflow|pipeline)\b',
                r'\b(hadoop|databricks|abinitio).*\b(and|vs|versus)\b.*\b(hadoop|databricks|abinitio)\b',
            ],
            QueryIntent.LINEAGE: [
                r'\b(lineage|trace|upstream|downstream|dependency|dependent)\b',
                r'\b(column|field|attribute).*\b(come from|source|derived|calculated|lineage|trace)\b',
                r'\b(data flow|data lineage)\b.*\b(source|target|derive|origin)\b',
                r'\bsttm\b.*\b(column|field|mapping)\b',  # STTM only with column context
            ],
            QueryIntent.LOGIC_ANALYSIS: [
                r'\b(logic|transformation|calculate|compute|process)\b',
                r'\b(how does|what does|explain|understand)\b.*\b(work|process|transform)\b',
                r'\b(business rule|rule|validation|check)\b',
                r'\b(aggregate|join|filter|group)\b',
            ],
            QueryIntent.CODE_SEARCH: [
                r'\b(find|search|locate|show me|where is)\b.*\b(code|function|component|graph)\b',
                r'\b(contains|uses|calls|references)\b',
                r'\b(all|list).*\b(graph|workflow|component|table)\b',
            ],
            QueryIntent.DOCUMENT_GENERATION: [
                r'\b(generate|create|export|produce|build)\b.*\b(document|doc|report|sheet|excel|sttm|mapping)\b',
                r'\b(generate|create).*\b(comparison|sttm|mapping)\b',
                r'\b(document|export)\b.*\b(for|about)\b.*\b(flow|workflow|pipeline)\b',
            ],
            QueryIntent.WORKFLOW_MAPPING: [
                r'\b(replaced|migration|unmapped|gap|mapping)\b.*\b(workflow|pipeline|flow)\b',
                r'\b(what.*replaced|which.*replaced)\b',
                r'\b(n:1|1:n|n-1|1-n|consolidation|split|merged|combined)\b',
                r'\b(show|list|find).*\b(mapping|unmapped)\b',
            ],
            QueryIntent.STAG_COMPARISON: [
                r'\b(stag|comparison report|migration report|assessment)\b',
                r'\b(generate|create|build|produce).*\bstag\b',
                r'\b(generate|create|build|produce).*\b(comparison|report|excel).*\b(hadoop|abinitio|databricks)\b',
                r'\b(full comparison|complete comparison|detailed comparison|comprehensive comparison)\b',
                r'\bcompare\b.*\b(abinitio|hadoop)\b.*\bto\b.*\bdatabricks\b',
                r'\bcompare\b.*\bdatabricks\b.*\bto\b.*\b(abinitio|hadoop)\b',
                r'\b(business stages|logic comparison)\b.*\b(report|excel|comparison)\b',
                r'\b(migration.*analysis|migration.*report|transformation.*report)\b',
            ],
        }

        # System detection patterns
        self.system_patterns = {
            'abinitio': r'\b(ab ?initio|\.mp|\.dml|graph|component)\b',
            'hadoop': r'\b(hadoop|hive|pig|\.hql|\.pig|mapreduce)\b',
            'databricks': r'\b(databricks|spark|pyspark|notebook|\.py|\.ipynb)\b',
        }

        # Comparison keywords
        self.comparison_keywords = [
            'compare', 'difference', 'versus', 'vs', 'similar', 'equivalent',
            'match', 'both', 'between', 'across', 'different', 'same'
        ]

        # Lineage keywords
        self.lineage_keywords = [
            'lineage', 'trace', 'track', 'flow', 'source', 'target', 'derive',
            'origin', 'upstream', 'downstream', 'dependency', 'column', 'field'
        ]

    def classify(self, query: str, context: Optional[Dict] = None) -> ClassifiedQuery:
        """
        Classify user query to determine intent and routing

        Args:
            query: User's question
            context: Optional context (previous conversation, selected entities, etc.)

        Returns:
            ClassifiedQuery with intent, entities, and suggested approach
        """
        query_lower = query.lower()

        # Extract entities and systems
        entities = self._extract_entities(query)
        systems = self._extract_systems(query)
        keywords = self._extract_keywords(query)

        # Score each intent
        intent_scores = {}
        for intent, patterns in self.patterns.items():
            score = 0
            for pattern in patterns:
                if re.search(pattern, query_lower, re.IGNORECASE):
                    score += 1
            intent_scores[intent] = score

        # Determine primary intent AND secondary intents
        intent, confidence, reasoning, secondary_intents = self._determine_intent(
            query_lower, intent_scores, entities, systems, keywords
        )

        # Suggest agents based on ALL intents (primary + secondary)
        all_intents = [intent] + secondary_intents
        suggested_agents = self._suggest_agents_multi_intent(all_intents, systems, entities)

        # Create task decomposition for multi-intent queries
        task_decomposition = self._create_task_plan_multi_intent(all_intents, query, entities, systems)

        return ClassifiedQuery(
            intent=intent,
            confidence=confidence,
            entities=entities,
            systems=systems,
            keywords=keywords,
            reasoning=reasoning,
            suggested_agents=suggested_agents,
            task_decomposition=task_decomposition,
            secondary_intents=secondary_intents,
            intent_scores=intent_scores
        )

    def _extract_entities(self, query: str) -> List[str]:
        """Extract entity names (graphs, tables, fields) from query"""
        entities = []

        # Pattern for quoted entities
        quoted = re.findall(r'["\']([^"\']+)["\']', query)
        entities.extend(quoted)

        # Pattern for capitalized entities or entities with underscores
        camel_or_snake = re.findall(r'\b([A-Z][a-zA-Z0-9_]*|[a-z]+_[a-z0-9_]+)\b', query)
        entities.extend(camel_or_snake)

        # Pattern for common entity keywords
        entity_patterns = [
            r'\bgraph\s+(\w+)',
            r'\btable\s+(\w+)',
            r'\bfield\s+(\w+)',
            r'\bcolumn\s+(\w+)',
            r'\bcomponent\s+(\w+)',
            r'\bworkflow\s+(\w+)',
        ]

        for pattern in entity_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            entities.extend(matches)

        return list(set(entities))

    def _extract_systems(self, query: str) -> List[str]:
        """Extract system types mentioned in query"""
        systems = []

        for system, pattern in self.system_patterns.items():
            if re.search(pattern, query, re.IGNORECASE):
                systems.append(system)

        return systems

    def _extract_keywords(self, query: str) -> List[str]:
        """Extract important keywords from query"""
        keywords = []

        # Check for comparison keywords
        for kw in self.comparison_keywords:
            if kw in query.lower():
                keywords.append(kw)

        # Check for lineage keywords
        for kw in self.lineage_keywords:
            if kw in query.lower():
                keywords.append(kw)

        return keywords

    def _determine_intent(
        self,
        query_lower: str,
        intent_scores: Dict,
        entities: List[str],
        systems: List[str],
        keywords: List[str]
    ) -> tuple[QueryIntent, float, str, List[QueryIntent]]:
        """Determine primary intent AND secondary intents with confidence and reasoning"""

        detected_intents = []  # List of (intent, confidence, reasoning)
        secondary_intents = []

        # STAG Comparison (HIGHEST PRIORITY - comprehensive reports)
        # Also catch "generate STAG for AbInitio X", "compare X to databricks", etc.
        stag_patterns = [
            r'\b(stag|full comparison|complete comparison|migration report|assessment)\b',
            r'\b(generate|create|build)\b.*\bstag\b',
            r'\b(generate|create)\b.*\b(comparison|compare).*\b(for|about|between)\b.*\b(abinitio|hadoop|databricks)',
            r'\bcompare\b.*\b(abinitio|hadoop)\b.*\bto\b.*\bdatabricks\b',
            r'\bcompare\b.*\bdatabricks\b.*\bto\b.*\b(abinitio|hadoop)\b',
        ]
        is_stag_request = any(re.search(pattern, query_lower, re.IGNORECASE) for pattern in stag_patterns)

        if is_stag_request:
            detected_intents.append((
                QueryIntent.STAG_COMPARISON,
                0.99,
                "Detected STAG comparison request - will generate comprehensive Excel report"
            ))

        # Document generation (HIGH PRIORITY)
        if any(kw in query_lower for kw in ['generate document', 'generate sttm', 'create document', 'export', 'generate report']):
            detected_intents.append((
                QueryIntent.DOCUMENT_GENERATION,
                0.98,
                "Detected document generation request - will generate docs/STTM"
            ))

        # Workflow mapping (HIGH PRIORITY)
        if any(kw in query_lower for kw in ['replaced', 'what replaced', 'n:1', '1:n', 'unmapped', 'migration', 'consolidation']):
            detected_intents.append((
                QueryIntent.WORKFLOW_MAPPING,
                0.96,
                "Detected workflow mapping query - will use intelligent workflow system"
            ))

        # Ab Initio-specific questions (CRITICAL FIX: Route to SIMPLE_RAG, not LINEAGE)
        # Questions like "What is graph X?", "What does X do?", "Show me X", "Explain X"
        abinitio_question_patterns = [
            r'\b(what is|what does|show me|explain|describe|tell me about)\b.*\b(graph|workflow|component|mp file)',
            r'\b(what is|what does)\b.*\b(\d{3,4}_[A-Z])',  # Match graph names like "1500_CDD_..."
            r'\bshow.*\b(vertices|flows|components|ports)\b',
            r'\bhow many\b.*\b(vertices|components|flows|ports)\b',
        ]
        is_abinitio_question = any(re.search(pattern, query_lower, re.IGNORECASE) for pattern in abinitio_question_patterns)

        # Boost SIMPLE_RAG for Ab Initio questions (avoid LINEAGE misclassification)
        if is_abinitio_question or ('abinitio' in systems and any(kw in query_lower for kw in ['what is', 'what does', 'show me', 'explain', 'describe'])):
            detected_intents.append((
                QueryIntent.SIMPLE_RAG,
                0.92,  # Higher than LINEAGE (0.90) to win priority
                "Detected Ab Initio information query - using vector search"
            ))

        # Multi-system comparison
        if len(systems) >= 2 and any(kw in query_lower for kw in self.comparison_keywords):
            detected_intents.append((
                QueryIntent.COMPARISON,
                0.95,
                f"Detected comparison query with {len(systems)} systems: {', '.join(systems)}"
            ))

        # Lineage query (CRITICAL FIX: Require column/field/data context, not just "source" or "target")
        # Must explicitly mention columns, fields, OR have explicit lineage keywords (lineage, trace, upstream, downstream)
        has_explicit_lineage = any(kw in query_lower for kw in ['lineage', 'trace', 'upstream', 'downstream', 'dependency', 'sttm'])
        has_column_context = any(kw in query_lower for kw in ['column', 'field', 'attribute', 'data flow', 'data lineage'])

        # Only trigger LINEAGE if we have explicit lineage keywords OR column context with source/target
        if has_explicit_lineage or (has_column_context and any(kw in query_lower for kw in ['source', 'target', 'derive', 'origin'])):
            detected_intents.append((
                QueryIntent.LINEAGE,
                0.90,
                f"Detected lineage/tracing query with keywords: {', '.join(keywords)}"
            ))

        # Logic analysis
        if any(kw in query_lower for kw in ['how does', 'explain', 'logic', 'transformation']):
            detected_intents.append((
                QueryIntent.LOGIC_ANALYSIS,
                0.85,
                "Detected logic analysis request - requires AI reasoning"
            ))

        # Code search
        if any(kw in query_lower for kw in ['find', 'search', 'locate', 'show me', 'list all']):
            detected_intents.append((
                QueryIntent.CODE_SEARCH,
                0.80,
                "Detected code search request - requires vector search"
            ))

        # Sort by confidence (highest first)
        detected_intents.sort(key=lambda x: x[1], reverse=True)

        # Primary intent is highest confidence
        if detected_intents:
            primary_intent, primary_confidence, primary_reasoning = detected_intents[0]

            # Secondary intents are additional intents with confidence > 0.75
            secondary_intents = [intent for intent, conf, _ in detected_intents[1:] if conf > 0.75]

            # If we have multiple high-confidence intents, update reasoning
            if secondary_intents:
                intent_names = ', '.join([i.value for i in secondary_intents])
                primary_reasoning += f" (Also detected: {intent_names})"

            return (primary_intent, primary_confidence, primary_reasoning, secondary_intents)

        # Check pattern scores as fallback
        if intent_scores:
            max_intent = max(intent_scores.items(), key=lambda x: x[1])
            if max_intent[1] > 0:
                return (
                    max_intent[0],
                    0.70,
                    f"Matched {max_intent[1]} patterns for {max_intent[0].value}",
                    []
                )

        # Default to simple RAG
        return (
            QueryIntent.SIMPLE_RAG,
            0.60,
            "No specific patterns matched - using simple RAG search",
            []
        )

    def _suggest_agents(self, intent: QueryIntent, systems: List[str], entities: List[str]) -> List[str]:
        """Suggest which agents to use based on intent (legacy - use _suggest_agents_multi_intent)"""
        agents = []

        if intent == QueryIntent.SIMPLE_RAG:
            agents = ["RAGAgent"]

        elif intent == QueryIntent.COMPARISON:
            agents = ["ParsingAgent", "SimilarityAgent", "ComparisonAgent"]

        elif intent == QueryIntent.LINEAGE:
            agents = ["ParsingAgent", "MappingAgent", "LineageAgent"]

        elif intent == QueryIntent.LOGIC_ANALYSIS:
            agents = ["ParsingAgent", "LogicAgent", "RAGAgent"]

        elif intent == QueryIntent.CODE_SEARCH:
            agents = ["RAGAgent", "ParsingAgent"]

        elif intent == QueryIntent.DOCUMENT_GENERATION:
            agents = ["DocumentGenerationAgent", "STTMGenerator", "ParsingAgent"]

        elif intent == QueryIntent.WORKFLOW_MAPPING:
            agents = ["WorkflowMappingAgent", "MigrationValidator"]

        elif intent == QueryIntent.STAG_COMPARISON:
            agents = ["STAGOrchestrator", "LogicExtractor", "BusinessAbstractor", "STTMGenerator", "ExcelGenerator"]

        elif intent == QueryIntent.MULTI_STEP:
            agents = ["RAGAgent", "ParsingAgent", "LogicAgent", "LineageAgent"]

        return agents

    def _suggest_agents_multi_intent(self, intents: List[QueryIntent], systems: List[str], entities: List[str]) -> List[str]:
        """Suggest agents for multi-intent queries (deduplicated)"""
        all_agents = []

        for intent in intents:
            agents = self._suggest_agents(intent, systems, entities)
            all_agents.extend(agents)

        # Remove duplicates while preserving order
        seen = set()
        unique_agents = []
        for agent in all_agents:
            if agent not in seen:
                seen.add(agent)
                unique_agents.append(agent)

        return unique_agents

    def _create_task_plan(
        self,
        intent: QueryIntent,
        query: str,
        entities: List[str],
        systems: List[str]
    ) -> List[str]:
        """Create task decomposition based on intent"""
        tasks = []

        if intent == QueryIntent.SIMPLE_RAG:
            tasks = [
                f"Search vector database for: '{query[:50]}...'",
                "Retrieve top 5 relevant chunks",
                "Generate answer from context"
            ]

        elif intent == QueryIntent.COMPARISON:
            system_list = ', '.join(systems) if systems else "detected systems"
            tasks = [
                f"Parse entities from {system_list}",
                f"Extract transformations and logic",
                f"Compare implementations using similarity scoring",
                f"Identify differences and similarities",
                f"Generate comparison report"
            ]

        elif intent == QueryIntent.LINEAGE:
            entity_list = ', '.join(entities[:3]) if entities else "specified entity"
            tasks = [
                f"Parse entity: {entity_list}",
                f"Extract column-level mappings (STTM)",
                f"Build dependency chains",
                f"Trace lineage across transformations",
                f"Generate lineage visualization data"
            ]

        elif intent == QueryIntent.LOGIC_ANALYSIS:
            entity_list = ', '.join(entities[:2]) if entities else "target entity"
            tasks = [
                f"Retrieve code for: {entity_list}",
                f"Extract transformation logic",
                f"Use AI to interpret business rules",
                f"Explain logic in natural language",
                f"Document findings"
            ]

        elif intent == QueryIntent.CODE_SEARCH:
            tasks = [
                f"Search codebase for pattern",
                f"Filter by systems: {', '.join(systems) if systems else 'all'}",
                f"Rank results by relevance",
                f"Extract code snippets",
                f"Present findings"
            ]

        elif intent == QueryIntent.DOCUMENT_GENERATION:
            entity_list = ', '.join(entities[:2]) if entities else "specified entity"
            tasks = [
                f"Find workflow/pipeline: {entity_list}",
                f"Extract complete metadata and transformations",
                f"Generate STTM mappings if needed",
                f"Create document (Excel/JSON/Markdown)",
                f"Provide download link"
            ]

        elif intent == QueryIntent.WORKFLOW_MAPPING:
            entity_list = ', '.join(entities[:2]) if entities else "workflows"
            tasks = [
                f"Load workflow intelligence data",
                f"Search for workflow mappings: {entity_list}",
                f"Calculate similarities and detect patterns (N:1, 1:N)",
                f"Generate answer with mapping details",
                f"Provide actionable recommendations"
            ]

        elif intent == QueryIntent.STAG_COMPARISON:
            entity_list = ', '.join(entities[:2]) if entities else "specified workflow"
            source_system = systems[0] if systems else "source system"
            tasks = [
                f"Identify source workflow: {entity_list}",
                f"Look up Databricks pipeline mapping",
                f"Extract {source_system} logic using vector search",
                f"Extract Databricks logic",
                f"Abstract to business stages (AI)",
                f"Generate STTM column mappings (AI)",
                f"Create comprehensive Excel comparison report"
            ]

        return tasks

    def _create_task_plan_multi_intent(
        self,
        intents: List[QueryIntent],
        query: str,
        entities: List[str],
        systems: List[str]
    ) -> List[str]:
        """Create task decomposition for multi-intent queries"""
        if not intents:
            return []

        # If single intent, use legacy method
        if len(intents) == 1:
            return self._create_task_plan(intents[0], query, entities, systems)

        # Multi-intent: combine tasks intelligently
        all_tasks = []

        # STAG comparison is comprehensive - if present, it includes most other intents
        if QueryIntent.STAG_COMPARISON in intents:
            # STAG already includes comparison, lineage, document generation
            all_tasks.extend(self._create_task_plan(QueryIntent.STAG_COMPARISON, query, entities, systems))

            # Add any additional specialized tasks not covered by STAG
            for intent in intents:
                if intent not in [QueryIntent.STAG_COMPARISON, QueryIntent.COMPARISON,
                                 QueryIntent.LINEAGE, QueryIntent.DOCUMENT_GENERATION]:
                    tasks = self._create_task_plan(intent, query, entities, systems)
                    all_tasks.extend(tasks)

        else:
            # No STAG - combine tasks from all intents
            for intent in intents:
                tasks = self._create_task_plan(intent, query, entities, systems)
                all_tasks.extend(tasks)

        # Remove duplicates while preserving order
        seen = set()
        unique_tasks = []
        for task in all_tasks:
            # Normalize task for comparison (lowercase, strip)
            normalized = task.lower().strip()
            if normalized not in seen:
                seen.add(normalized)
                unique_tasks.append(task)

        return unique_tasks


def create_query_classifier(ai_analyzer=None) -> QueryClassifier:
    """Factory function to create query classifier"""
    return QueryClassifier(ai_analyzer=ai_analyzer)
